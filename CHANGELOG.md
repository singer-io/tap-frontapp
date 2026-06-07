# Changelog

## 3.1.0
  * Exclude Streams that the credentials cannot access (403) from the catalog during discovery; discovery fails only if the credentials cannot read any supported parent stream.[#42](https://github.com/singer-io/tap-frontapp/pull/42)

## 3.0.0
  * Upgrade `singer-python` to `6.8.0` and `requests` to `2.33.0`
  * Schema Update. [#39](https://github.com/singer-io/tap-frontapp/pull/39)

## 2.2.0
  * Upgraded dependencies versions and added unit tests [#38](https://github.com/singer-io/tap-frontapp/pull/38)

## 2.1.0
  * Bump dependency versions for twistlock compliance [#37](https://github.com/singer-io/tap-frontapp/pull/37)

## 2.0.1
  * Bump dependency versions for twistlock compliance [#31](https://github.com/singer-io/tap-frontapp/pull/31)

## 2.0.0
  * Implementing new Frontapp [Analytics API](https://dev.frontapp.com/changelog/core-api-analytics-and-exports-updates) [#17](https://github.com/singer-io/tap-frontapp/pull/17)

## 1.1.1
  * Dependabot update [#18](https://github.com/singer-io/tap-frontapp/pull/18)
  * Setup circleci project [#21](https://github.com/singer-io/tap-frontapp/pull/21)

## 1.1.0
  * Add several new analytics tables [#12](https://github.com/singer-io/tap-frontapp/pull/12)

## 1.0.4
  * Increase polling interval due to issues with rate limiting [#4](https://github.com/singer-io/tap-frontapp/pull/4)

## 1.0.3
  * Remove `metric` config param [#1](https://github.com/singer-io/tap-frontapp/pull/1)

## 1.0.2
  * Fix an issue that causes `discovery` mode to fail

## 1.0.1
  * Pin `singer-python` to version 5.4.0
  * Pin `backoff` to version 1.3.2

## 1.0.0
  * General release of the tap

## 0.3.2
  * Update version of `requests` to `0.20.0` in response to CVE 2018-18074
