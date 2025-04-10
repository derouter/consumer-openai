import * as v from "valibot";

const PriceSchema = v.object({
  $pol: v.pipe(
    v.string(),
    v.check((x) => {
      let num = parseFloat(x);
      if (Number.isNaN(num)) return false;
      if (num < 0) return false;
      return true;
    }, "Must be parsed as a positive number"),
  ),
});

export const ConfigSchema = v.object({
  server: v.object({
    host: v.optional(v.string(), "localhost"),

    port: v.pipe(
      v.number(),
      v.integer(),
      v.minValue(0),
      v.maxValue(2 ** 16 - 1),
    ),

    models: v.record(
      v.string(),
      v.object({
        minContextSize: v.pipe(v.number(), v.integer(), v.minValue(1)),
        maxInputPrice: PriceSchema,
        maxOutputPrice: PriceSchema,
        minTrialAllowance: v.optional(PriceSchema),
      }),
    ),
  }),

  rpc: v.optional(
    v.object({
      host: v.optional(v.string(), "127.0.0.1"),
      port: v.optional(v.number(), 4269),
    }),
    {
      host: "127.0.0.1",
      port: 4269,
    },
  ),

  autoDeposit: v.optional(
    v.record(
      v.string(),
      v.object({
        treshold: v.string(),
        amount: v.string(),
      }),
    ),
  ),
});
