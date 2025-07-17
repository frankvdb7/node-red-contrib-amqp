module.exports = [
  {
    ignores: ['build/', '**/*.html', 'src/nodes/icons'],
  },
  {
    files: ['**/*.ts'],
    languageOptions: {
      parser: require('@typescript-eslint/parser'),
      parserOptions: {
        ecmaVersion: 2018,
        sourceType: 'module',
      },
    },
    plugins: {
      '@typescript-eslint': require('@typescript-eslint/eslint-plugin'),
      prettier: require('eslint-plugin-prettier'),
    },
    rules: {
      semi: ['error', 'never'],
      'no-console': 'off',
      'arrow-parens': ['warn', 'as-needed'],
      'no-lone-blocks': 'off',
      '@typescript-eslint/ban-ts-comment': 'warn',
      '@typescript-eslint/no-unused-expressions': 'off',
    },
  },
]
