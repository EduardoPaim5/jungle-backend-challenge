import { checkEnvironment, formatEnvironmentIssues } from './environment.js';

const issues = await checkEnvironment();
if (issues.length > 0) {
  console.error(formatEnvironmentIssues(issues));
  process.exitCode = 1;
} else {
  console.log('Ambiente pronto: PostgreSQL, schema e filas SQS verificados.');
}
