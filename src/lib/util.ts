import * as bignumber from "@ethersproject/bignumber";

export const pick = <T extends {}, K extends keyof T>(obj: T, keys: Array<K>) =>
  Object.fromEntries(
    keys.filter((key) => key in obj).map((key) => [key, obj[key]]),
  ) as Pick<T, K>;

export const omit = <T extends {}, K extends keyof T>(obj: T, keys: Array<K>) =>
  Object.fromEntries(
    Object.entries(obj).filter(([key]) => !keys.includes(key as K)),
  ) as Omit<T, K>;

/**
 * @example parseEther("1.0") === BigNumber.from("1000000000000000000")
 */
export function parseEther(value: string): bignumber.BigNumber {
  return bignumber.parseFixed(value, 18);
}

/**
 * @example formatEther(BigNumber.from("1000000000000000000")) === "1.0"
 */
export function formatEther(value: bignumber.BigNumber): string {
  return bignumber.formatFixed(value, 18);
}

/**
 * @example parseWeiToEth("1000000000000000000") === 1.0
 */
export function parseWeiToEth(value: string): number {
  return parseFloat(formatEther(bignumber.BigNumber.from(value)));
}

/**
 * Returns a new object with entries sorted by key.
 */
export function sortByKey<T extends { [key: string]: any }>(
  obj: T,
  compareFn?: (a: string, b: string) => number,
): T {
  return Object.keys(obj)
    .sort(compareFn)
    .reduce((acc, key) => {
      (acc as any)[key] = obj[key];
      return acc;
    }, {} as T);
}
