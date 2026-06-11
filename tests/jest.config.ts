import type {Config} from '@jest/types';
const config: Config.InitialOptions = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testTimeout: 120000,
  roots: ['<rootDir>/integration']
};
export default config;
