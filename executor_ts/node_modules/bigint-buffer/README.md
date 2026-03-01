# 💪🔢 bigint-buffer: Buffer Utilities for TC39 BigInt Proposal (FIXED VERSION)
[![NPM Package](https://img.shields.io/npm/v/@y3fers0n/bigint-buffer.svg?style=flat-square)](https://www.npmjs.org/package/@y3fers0n/bigint-buffer)

## SECURITY NOTICE: Fixed Version

This is a fork of the original [bigint-buffer](https://www.npmjs.org/package/bigint-buffer) package with a fix for [CVE-2025-3194](https://github.com/advisories/GHSA-3gc7-fjrx-p6mg), a Buffer Overflow vulnerability in the `toBigIntLE()` function that could allow attackers to crash the application.

### Vulnerability Fixed
- The original package was vulnerable to Buffer Overflow via `toBigIntLE()` function
- This fork adds proper input validation to prevent crashes when passing `null` or non-Buffer inputs
- Use this version to replace the original `bigint-buffer` package in your dependencies

## Original Description

[bigint-buffer](https://www.npmjs.org/package/bigint-buffer) is a utility converts [TC39 Proposed BigInts](https://github.com/tc39/proposal-bigint) to and from buffers. This utility is necessary because BigInts, as proposed, do not support direct conversion between Buffers (or UInt8Arrays), but rather require conversion from buffers to hexadecimal strings then to BigInts, which is suboptimal. This utility includes N-API bindings, so under node, conversion is performed without generating a hexadecimal string. In the browser, normal string conversion is used.

# Why use BigInts?

BigInts are currently a stage 3 proposal, supported in Node 10 and V8 v6.7. BigInts are primitive arbitrary precision integers, overcoming the limitations of the number type in javascript, which only supports up to 53 bits of precision. 

In many applications, manipulating 64, 128 or even 256 bit numbers is quite common. For example, database identifiers are often 128 bits, and hashes are often 256 bits (If you're looking for hashing support, try out [bigint-hash](https://github.com/no2chem/bigint-hash)). Before BigInts, manipulating these numbers safely required either allocating a Buffer or UInt8Arrays, which is quite expensive compared to a number, since Buffers are allocated on the heap. 

BigInts solve this problem by introducing a primitive that can hold
arbitrary precision integers, reducing memory pressure and allowing
the runtime to better optimize arithmetic operations. This results in significant performance improvements - 10x-100x for simple equality comparisons (using `===` vs `Buffer.compare()`):

```
Buffer equality comparison: 11916844±4.23% ops/s 83.91±17.293 ns/op (91 runs)
bigint equality comparison: 798024851±0.29% ops/s 1.25±0.017 ns/op (91 runs)
```

Before BigInts, you probably used a library such as the widely used [bn.js](https://www.npmjs.com/package/bn.js).
bn.js fares a little better than a plain Buffer, but is still 5-10x slower than the bigint:
```
BN equality comparison: 73255774±0.67% ops/s 13.65±0.442 ns/op (89 runs)
```

bigints are also much better with arithmetic, here are the results compared to BN for multiplying two
128-bit integers, yielding a 4x improvement:
```
BN multiply: 4763236±0.49% ops/s 209.94±5.111 ns/op (93 runs)
bigint multiply: 15268666±0.92% ops/s 65.49±2.938 ns/op (92 runs)
```
