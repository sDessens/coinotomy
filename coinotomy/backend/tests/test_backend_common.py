def generate_n(n):
    return [(i, i / 2.0, i * 2.0) for i in range(n)]


class CommonBackend():

    def test_file_is_empty(self):
        assert([] == list(self.backend.lines()))

    def test_can_append_1_line(self):
        for ts, p, v in generate_n(1):
            self.backend.append(ts, p, v)

        assert list(self.backend.lines()) == generate_n(1)

    def test_can_append_100_lines(self):
        for ts, p, v in generate_n(100):
            self.backend.append(ts, p, v)

        assert list(self.backend.lines()) == generate_n(100)

    def test_can_append_10000_lines(self):
        for ts, p, v in generate_n(10000):
            self.backend.append(ts, p, v)

        assert list(self.backend.lines()) == generate_n(10000)

    def test_can_append_1_line_reverse(self):
        for ts, p, v in generate_n(1):
            self.backend.append(ts, p, v)

        assert list(self.backend.rlines()) == generate_n(1)[::-1]

    def test_can_append_100_lines_reverse(self):
        for ts, p, v in generate_n(100):
            self.backend.append(ts, p, v)

        assert list(self.backend.rlines()) == generate_n(100)[::-1]

    def test_can_append_10000_lines_reverse(self):
        for ts, p, v in generate_n(10000):
            self.backend.append(ts, p, v)

        assert list(self.backend.rlines()) == generate_n(10000)[::-1]

    def test_can_append_after_query(self):
        expected = generate_n(10)
        expected_1 = expected[:4]
        expected_2 = expected[4:]

        for ts, p, v in expected_1:
            self.backend.append(ts, p, v)

        assert list(self.backend.lines()) == expected_1

        for ts, p, v in expected_2:
            self.backend.append(ts, p, v)

        assert list(self.backend.lines()) == (expected_1 + expected_2)

    def test_limits(self):
        # definitely still want ms precision in 2030
        lines = [
            (1905679642.0000, 0, 0),
            (1905679642.0001, 0, 0),
            (1905679642.0002, 0, 0)
        ]
        self.assertNotEqual(lines[0][0], lines[1][0])
        self.assertNotEqual(lines[1][0], lines[2][0])

        for ts, p, v in lines:
            self.backend.append(ts, p, v)

        self.assertEqual(list(self.backend.lines()), lines)

    def test_reopen(self):
        self.backend.append(1, 2, 3)

        self.backend.unload()
        self.backend = self._create()

        self.backend.append(4, 5, 6)
        lines = list(self.backend.lines())
        self.assertEqual([(1, 2, 3), (4, 5, 6)], lines)