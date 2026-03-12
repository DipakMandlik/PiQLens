import test from 'node:test';
import assert from 'node:assert/strict';
import { computeSuccessRate, ensureNonNegativeNumber, formatTimestampIST } from '../format-utils';

test('computeSuccessRate handles zero denominator', () => {
  assert.equal(computeSuccessRate(0, 0), 0);
});

test('computeSuccessRate rounds to 2 decimals', () => {
  assert.equal(computeSuccessRate(7, 9), 77.78);
});

test('ensureNonNegativeNumber clamps negatives and invalid', () => {
  assert.equal(ensureNonNegativeNumber(-2), 0);
  assert.equal(ensureNonNegativeNumber('abc'), 0);
  assert.equal(ensureNonNegativeNumber(5), 5);
});

test('formatTimestampIST returns IST-formatted timestamp', () => {
  const output = formatTimestampIST('2026-02-20T00:00:00Z');
  assert.match(output, /IST$/);
});
