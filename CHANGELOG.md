# Changelog

## 1.0.5
 * Remove `paginationDetails` parameter from select paged API calls; no longer needed after recent change to Mambu API. This change requested by Mambu to improve Mambu API server performance.

## 1.0.4
 * Fix pagination [#11](https://github.com/singer-io/tap-mambu/pull/11)

## 1.0.3
 * Added json schemas for loan_repayments.

## 1.0.2
 * Fix merge issue with `v.1.0.1`. Revert to `v.1.0.0 `and re-apply pagination changes.

## 1.0.1
 * Change `client.py` and `sync.py` to remove `Items-Total` response header from pagination logic.

## 1.0.0
 * Releasing from Beta --> GA

## 0.0.5
  * Add stream handling for GL Account and GL Journal Entries endpoints

## 0.0.4
  * Update sync.py date handling in process_records and in sync bookmarks for POST endpoints

## 0.0.3
  * Move Transformer initialization "up a level" to remove noise from logging

## 0.0.2
  * Adjust json schemas for loan_products, deposit_products for custom_field_values and other array/lists

## 0.0.1
  * Initial commit
