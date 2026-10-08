import {
  Body,
  Catch,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpException,
  Inject,
  Injectable,
  Module,
  Param,
  Post,
  Query,
  Req,
  Res,
  SetMetadata,
  type ArgumentsHost,
  type CanActivate,
  type DynamicModule,
  type ExecutionContext,
  type ExceptionFilter,
} from '@nestjs/common';
import { APP_GUARD, Reflector } from '@nestjs/core';
import { ApiHeader, ApiOperation, ApiProperty, ApiResponse, ApiTags } from '@nestjs/swagger';
import type { FastifyReply, FastifyRequest } from 'fastify';
import { Wagering, transient } from '../application/wagering.js';
import { Queries } from '../application/queries.js';
import { parseCommand, resultHttpStatus, ServiceError } from '../application/contracts.js';
import { DomainError } from '../domain/errors.js';
import { Database, sqlState } from '../infrastructure/database.js';
import { Queues } from '../infrastructure/sqs.js';
import { Observability, logger } from '../infrastructure/observability.js';

export class MoneyDto {
  @ApiProperty({ example: '100.00', pattern: '^\\d+\\.\\d{2}$', type: String }) amount!: string;
  @ApiProperty({ enum: ['BRL'], example: 'BRL' }) currency!: string;
}
export class WalletInputDto {
  @ApiProperty({ format: 'uuid' }) playerId!: string;
  @ApiProperty({ type: MoneyDto }) initialBalance!: MoneyDto;
}
export class WagerInputDto {
  @ApiProperty({ example: 'jungle' }) providerId!: string;
  @ApiProperty({ example: 'bet-001' }) externalTransactionId!: string;
  @ApiProperty({ format: 'uuid' }) playerId!: string;
  @ApiProperty({ format: 'uuid' }) walletId!: string;
  @ApiProperty({ example: 'round-001' }) roundId!: string;
  @ApiProperty({ example: 'game-001' }) gameId!: string;
  @ApiProperty({ enum: ['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'] }) kind!: string;
  @ApiProperty({ type: MoneyDto }) money!: MoneyDto;
  @ApiProperty({
    required: false,
    example: 'bet-001',
    description: 'Obrigatória para REFUND/ROLLBACK; opcional para WIN.',
  })
  referenceExternalTransactionId?: string;
}
function correlation(req: FastifyRequest): string {
  return req.id;
}

/** Replace this port with OIDC/JWKS verification and provider/wallet authorization in a deployment. */
export abstract class IdentityPort {
  abstract identify(req: FastifyRequest): Promise<{ subject: string; mode: string }>;
}
export class DevelopmentIdentity extends IdentityPort {
  override async identify(_req: FastifyRequest) {
    return { subject: 'local-development', mode: 'development' };
  }
}
const PUBLIC_ENDPOINT = 'jungle:public-endpoint';
@Injectable()
export class IdentityGuard implements CanActivate {
  constructor(
    @Inject(IdentityPort) private readonly identity: IdentityPort,
    private readonly reflector: Reflector,
  ) {}
  async canActivate(context: ExecutionContext): Promise<boolean> {
    if (
      this.reflector.getAllAndOverride<boolean>(PUBLIC_ENDPOINT, [context.getHandler(), context.getClass()])
    )
      return true;
    await this.identity.identify(context.switchToHttp().getRequest<FastifyRequest>());
    return true;
  }
}

@Controller()
@ApiTags('Wagering')
export class ApiController {
  constructor(
    private readonly wagering: Wagering,
    private readonly queries: Queries,
    private readonly db: Database,
    private readonly queues: Queues,
    private readonly metrics: Observability,
  ) {}

  @Post('wallets')
  @ApiOperation({ summary: 'Abrir wallet; crédito inicial e eventos são atômicos.' })
  @ApiResponse({ status: 201, description: 'Wallet criada com versão inicial 1.' })
  @ApiResponse({ status: 409, description: 'Já existe uma wallet para jogador/moeda.' })
  async createWallet(@Body() body: WalletInputDto, @Req() req: FastifyRequest) {
    return this.wagering.openWallet(body, correlation(req));
  }
  @Get('wallets/:walletId')
  wallet(@Param('walletId') id: string) {
    return this.queries.wallet(id);
  }
  @Get('wallets/:walletId/ledger')
  ledger(@Param('walletId') id: string, @Query('cursor') cursor?: string, @Query('limit') limit?: string) {
    return this.queries.ledger(id, cursor, limit);
  }
  @Get('wagering/transactions/:transactionId')
  transaction(@Param('transactionId') id: string) {
    return this.queries.transaction(id);
  }
  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  external(@Param('providerId') provider: string, @Param('externalTransactionId') external: string) {
    return this.queries.external(provider, external);
  }
  @Post('wagering/transactions')
  @ApiHeader({
    name: 'Idempotency-Key',
    required: true,
    description: 'Reutilizar a mesma chave e payload após resposta 503.',
  })
  @ApiResponse({ status: 200, description: 'PROCESSED ou replay do resultado original.' })
  @ApiResponse({ status: 202, description: 'PENDING_REFERENCE; worker persistente finalizará a operação.' })
  @ApiResponse({ status: 400, description: 'Contrato inválido.' })
  @ApiResponse({ status: 409, description: 'Conflito de idempotência/identidade externa.' })
  @ApiResponse({ status: 422, description: 'REJECTED com failureCode e saldo observado.' })
  @ApiResponse({ status: 500, description: 'Falha permanente registrada ou falha interna.' })
  @ApiResponse({ status: 503, description: 'Resultado inconclusivo: repetir a mesma chave e payload.' })
  async submit(
    @Body() body: WagerInputDto,
    @Headers('idempotency-key') key: unknown,
    @Req() req: FastifyRequest,
    @Res({ passthrough: true }) reply: FastifyReply,
  ) {
    const result = await this.wagering.process(parseCommand(body, key), {
      source: 'http',
      correlationId: correlation(req),
    });
    reply.status(resultHttpStatus(result));
    return result;
  }
  @Post('wallets/:walletId/reconciliation')
  @HttpCode(200)
  reconciliation(@Param('walletId') id: string, @Req() req: FastifyRequest) {
    return this.queries.reconciliation(id, correlation(req));
  }
  @Get('health/live')
  @SetMetadata(PUBLIC_ENDPOINT, true)
  live() {
    return { status: 'ok', authMode: process.env.AUTH_MODE ?? 'development' };
  }
  @Get('health/ready')
  @SetMetadata(PUBLIC_ENDPOINT, true)
  async ready() {
    try {
      await Promise.all([this.db.query('SELECT 1'), this.queues.ready()]);
      return { status: 'ready', postgres: 'ok', sqs: 'ok' };
    } catch {
      throw new ServiceError('DEPENDENCY_UNAVAILABLE', 503, true);
    }
  }
  @Get('metrics')
  async prometheus(@Res({ passthrough: true }) reply: FastifyReply) {
    reply.type(this.metrics.registry.contentType);
    return this.metrics.registry.metrics();
  }
}

@Catch()
export class ApiErrorFilter implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost): void {
    const http = host.switchToHttp();
    const reply = http.getResponse<FastifyReply>();
    const req = http.getRequest<FastifyRequest>();
    const status =
      error instanceof ServiceError
        ? error.status
        : error instanceof DomainError
          ? 400
          : error instanceof HttpException
            ? error.getStatus()
            : transient(error)
              ? 503
              : 500;
    const code =
      error instanceof ServiceError || error instanceof DomainError
        ? error.code
        : error instanceof HttpException
          ? 'HTTP_ERROR'
          : status === 503
            ? 'INFRASTRUCTURE_UNAVAILABLE'
            : 'INTERNAL_ERROR';
    logger[status >= 500 ? 'error' : 'warn'](
      { correlationId: req.id, errorCode: code, sqlState: sqlState(error), status },
      'http_request_failed',
    );
    if (status === 503) reply.header('Retry-After', '1');
    void reply.status(status).send({
      code,
      correlationId: req.id,
      retryable: status === 503,
      ...(status === 503
        ? {
            retryInstruction:
              'Reenvie a mesma Idempotency-Key e o mesmo payload; o resultado anterior pode ter sido confirmado.',
          }
        : {}),
    });
  }
}
@Module({})
export class ApiModule {
  static configure(
    db: Database,
    queues: Queues,
    metrics: Observability,
    wagering: Wagering,
    identity: IdentityPort = new DevelopmentIdentity(),
  ): DynamicModule {
    return {
      module: ApiModule,
      controllers: [ApiController],
      providers: [
        { provide: Database, useValue: db },
        { provide: Queues, useValue: queues },
        { provide: Observability, useValue: metrics },
        { provide: Wagering, useValue: wagering },
        { provide: Queries, useValue: new Queries(db, metrics) },
        { provide: IdentityPort, useValue: identity },
        { provide: APP_GUARD, useClass: IdentityGuard },
      ],
    };
  }
}
