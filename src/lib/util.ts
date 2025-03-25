/**
 * Would panic upon non-exhaustive-ness.
 * @example default: unreachable(case)
 */
export function unreachable(arg: never) {
  return arg;
}

/**
 * An object promised to be resolved in the future.
 *
 * @example
 * const deferred = new Deferred<number>();
 * deferred.promise.then(console.log);
 * deferred.resolve(42);
 */
export class Deferred<T> {
  readonly promise: Promise<T>;
  private _resolve!: (value: T | PromiseLike<T>) => void;
  private _reject!: (reason: any) => void;
  private _resolved = false;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this._resolve = resolve;
      this._reject = reject;
    });
  }

  get resolved() {
    return this._resolved;
  }

  resolve(value: T | PromiseLike<T>): void {
    this._resolved = true;
    this._resolve(value);
  }

  reject(reason: any): void {
    this._reject(reason);
  }
}
