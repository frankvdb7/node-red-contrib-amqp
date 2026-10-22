require('@swc/register')({
  jsc: { target: 'es2022' },
  module: { type: 'commonjs' },
  sourceMaps: 'inline',
  sourceRoot: '',
  inlineSourcesContent: true,
});
