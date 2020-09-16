class RandomGenerator:

    mask32 = 11111111111111111111111111111111
    seed_skip = 128 * 1024 * 1024
    seeds = [0,
             4160749568,
             4026531840,
             3892314112,
             3758096384,
             3623878656,
             3489660928,
             3355443200,
             3221225472,
             3087007744,
             2952790016,
             2818572288,
             2684354560,
             2550136832,
             2415919104,
             2281701376,
             2147483648,
             2013265920,
             1879048192,
             1744830464,
             1610612736,
             1476395008,
             1342177280,
             1207959552,
             1073741824,
             939524096,
             805306368,
             671088640,
             536870912,
             402653184,
             268435456,
             134217728
             ]

    def __init__(self, initial_iteration):
        base_index = int(int(initial_iteration & RandomGenerator.mask32) / RandomGenerator.seed_skip)
        self.seed = RandomGenerator.seeds[base_index]
        for i in range(0, initial_iteration % RandomGenerator.seed_skip):
            self.next()

    def next(self):
        self.seed = (self.seed * 314159261 + 663896637) & RandomGenerator.mask32
        return self.seed

