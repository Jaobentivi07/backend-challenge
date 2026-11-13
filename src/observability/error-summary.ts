/** Registra tipo e código do erro sem expor mensagem, SQL, payload ou credenciais. */
export function resumirErroParaLog(error: unknown): Record<string, string> {
  if (!(error instanceof Error)) return { errorName: 'UnknownError' };
  const erroEstruturado = error as Error & { code?: unknown; cause?: { code?: unknown } };
  const codigo = erroEstruturado.code ?? erroEstruturado.cause?.code;
  return {
    errorName: error.name,
    ...(typeof codigo === 'string' ? { errorCode: codigo } : {}),
  };
}
