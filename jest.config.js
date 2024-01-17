/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  globalTeardown: "<rootDir>/src/jestGlobalTeardown.ts",
  globalSetup: "<rootDir>/src/jestGlobalSetup.ts",
};
