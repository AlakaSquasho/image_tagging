import io
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PIL import Image

from image_collage import build_collage_image, compute_grid


class ComputeGridTest(unittest.TestCase):
    def test_expected_layouts(self):
        expected = {
            0: (0, 0),
            1: (1, 1),
            2: (2, 1),
            3: (2, 2),
            4: (2, 2),
            5: (3, 2),
            6: (3, 2),
            7: (3, 3),
            9: (3, 3),
        }
        for count, grid in expected.items():
            with self.subTest(count=count):
                self.assertEqual(compute_grid(count), grid)

    def test_nine_is_three_by_three(self):
        self.assertEqual(compute_grid(9), (3, 3))


class BuildCollageImageTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmpdir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def _path(self, name):
        return os.path.join(self.tmpdir, name)

    def _make_heterogeneous_images(self):
        # 9 张不同尺寸与模式的图片
        paths = []
        Image.new("RGB", (2000, 1200), (200, 30, 30)).save(self._path("a.jpg"), "JPEG")
        paths.append(self._path("a.jpg"))
        Image.new("RGBA", (800, 1600), (30, 200, 30, 255)).save(self._path("b.png"), "PNG")
        paths.append(self._path("b.png"))
        Image.new("P", (300, 300)).save(self._path("c.gif"), "GIF")
        paths.append(self._path("c.gif"))
        Image.new("CMYK", (400, 400)).save(self._path("d.jpg"), "JPEG")
        paths.append(self._path("d.jpg"))
        Image.new("RGB", (40, 40), (0, 0, 200)).save(self._path("e.jpg"), "JPEG")
        paths.append(self._path("e.jpg"))
        for i in range(4):
            Image.new("RGB", (600 + i, 500 + i), (100, 100, 100)).save(self._path(f"f{i}.jpg"), "JPEG")
            paths.append(self._path(f"f{i}.jpg"))
        return paths

    def test_nine_images_produce_three_by_three_jpeg(self):
        cell = 64
        result = build_collage_image(self._make_heterogeneous_images(), cell_size=cell)
        self.assertIsNotNone(result)
        with Image.open(io.BytesIO(result)) as image:
            self.assertEqual(image.size, (3 * cell, 3 * cell))
            self.assertEqual(image.format, "JPEG")
            self.assertEqual(image.mode, "RGB")

    def test_alpha_is_composited_onto_background(self):
        background = (10, 20, 30)
        transparent = Image.new("RGBA", (80, 160), (255, 0, 0, 0))
        transparent.save(self._path("t.png"), "PNG")
        opaque = Image.new("RGB", (100, 100), (200, 200, 200))
        opaque.save(self._path("o.jpg"), "JPEG")

        result = build_collage_image(
            [self._path("t.png"), self._path("o.jpg")],
            cell_size=64,
            background=background,
        )
        self.assertIsNotNone(result)
        with Image.open(io.BytesIO(result)) as image:
            # 第一格整张透明，应完全等于背景色
            self.assertEqual(image.getpixel((32, 32)), background)

    def test_missing_files_are_skipped_and_grid_shrinks(self):
        Image.new("RGB", (100, 100), (1, 2, 3)).save(self._path("x.jpg"), "JPEG")
        Image.new("RGB", (100, 100), (3, 2, 1)).save(self._path("y.jpg"), "JPEG")
        Image.new("RGB", (100, 100), (2, 1, 3)).save(self._path("z.jpg"), "JPEG")
        paths = [self._path("x.jpg"), self._path("y.jpg"), self._path("z.jpg"), self._path("missing.jpg")]

        cell = 64
        result = build_collage_image(paths, cell_size=cell)
        self.assertIsNotNone(result)
        with Image.open(io.BytesIO(result)) as image:
            # 只有 3 张可用 -> 2x2 网格
            self.assertEqual(image.size, (2 * cell, 2 * cell))

    def test_all_missing_returns_none(self):
        self.assertIsNone(build_collage_image([self._path("nope1.jpg"), self._path("nope2.jpg")]))

    def test_corrupt_file_returns_none(self):
        with open(self._path("broken.jpg"), "wb") as handle:
            handle.write(b"not an image at all")
        self.assertIsNone(build_collage_image([self._path("broken.jpg")]))

    def test_single_column_override(self):
        paths = []
        for i in range(3):
            Image.new("RGB", (100, 100), (i, i, i)).save(self._path(f"c{i}.jpg"), "JPEG")
            paths.append(self._path(f"c{i}.jpg"))

        cell = 64
        result = build_collage_image(paths, cell_size=cell, columns=1)
        self.assertIsNotNone(result)
        with Image.open(io.BytesIO(result)) as image:
            self.assertEqual(image.size, (cell, cell * 3))

    def test_cell_size_is_clamped(self):
        paths = []
        for i in range(9):
            Image.new("RGB", (100, 100), (i, i, i)).save(self._path(f"k{i}.jpg"), "JPEG")
            paths.append(self._path(f"k{i}.jpg"))

        result = build_collage_image(paths, cell_size=99999)
        self.assertIsNotNone(result)
        with Image.open(io.BytesIO(result)) as image:
            self.assertEqual(image.size, (3 * 1024, 3 * 1024))


if __name__ == "__main__":
    unittest.main()
