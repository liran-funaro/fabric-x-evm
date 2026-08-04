"""Tests for the demo video frame schedule.

Run: cd scripts/demo && python3 -m unittest test_demo_lib -v

The invariant that matters most is test_full_mode_is_exactly_real_time: the whole
point of the full video is that it is not sped up, and a drift between the frame
step and the frame's on-screen duration would silently produce a fast-forwarded
video that still looks plausible.
"""

import unittest

import demo_lib


class TestFullMode(unittest.TestCase):
    def test_is_exactly_real_time(self):
        # 60s of run at 0.5 content fps -> 30 frames, each held 2s => 60s of video.
        frames = demo_lib.frame_schedule("full", start=1000.0, end=1060.0)
        self.assertEqual(len(frames), 30)
        video_seconds = len(frames) / demo_lib.FULL_CONTENT_FPS
        self.assertAlmostEqual(video_seconds, 60.0, delta=0.01,
                               msg="full mode must be 1x real time")

    def test_step_matches_frame_duration(self):
        # The step is DERIVED from the content fps, never an independent input.
        frames = demo_lib.frame_schedule("full", start=0.0, end=100.0)
        step_ms = frames[1][1] - frames[0][1]
        self.assertAlmostEqual(step_ms, 1000.0 / demo_lib.FULL_CONTENT_FPS, places=6)

    def test_sliding_window_is_fixed_width(self):
        for fr, to in demo_lib.frame_schedule("full", start=0.0, end=100.0):
            self.assertAlmostEqual(to - fr, demo_lib.WINDOW_SECONDS * 1000, places=6)

    def test_last_frame_reaches_the_end(self):
        frames = demo_lib.frame_schedule("full", start=0.0, end=100.0)
        self.assertLessEqual(frames[-1][1] / 1000.0, 100.0)
        self.assertGreater(frames[-1][1] / 1000.0, 100.0 - 1.0 / demo_lib.FULL_CONTENT_FPS - 0.001)

    def test_eight_hour_run_frame_count(self):
        frames = demo_lib.frame_schedule("full", start=0.0, end=8 * 3600.0)
        self.assertEqual(len(frames), int(8 * 3600 * demo_lib.FULL_CONTENT_FPS))


class TestHighlightMode(unittest.TestCase):
    def test_covers_whole_run(self):
        end = 8 * 3600.0
        frames = demo_lib.frame_schedule("highlight", start=0.0, end=end)
        self.assertAlmostEqual(frames[-1][1] / 1000.0, end, delta=60.0,
                               msg="highlight must sweep to the run's end")

    def test_fits_target_length(self):
        frames = demo_lib.frame_schedule("highlight", start=0.0, end=8 * 3600.0)
        video_seconds = len(frames) / demo_lib.HIGHLIGHT_CONTENT_FPS
        self.assertGreaterEqual(video_seconds, 60.0)
        self.assertLessEqual(video_seconds, 120.0)

    def test_accelerates(self):
        frames = demo_lib.frame_schedule("highlight", start=0.0, end=8 * 3600.0)
        first = frames[1][1] - frames[0][1]
        last = frames[-1][1] - frames[-2][1]
        self.assertGreater(last, first * 5, "highlight must ramp from slow to fast")

    def test_opens_no_faster_than_scrape_resolution(self):
        # The first step must not be finer than the loadgen scrape interval, or
        # consecutive frames render identical data.
        frames = demo_lib.frame_schedule("highlight", start=0.0, end=8 * 3600.0)
        first_step_s = (frames[1][1] - frames[0][1]) / 1000.0
        self.assertGreaterEqual(first_step_s, demo_lib.SCRAPE_INTERVAL_SECONDS - 1e-9)

    def test_monotonic(self):
        frames = demo_lib.frame_schedule("highlight", start=0.0, end=8 * 3600.0)
        tos = [to for _, to in frames]
        self.assertEqual(tos, sorted(tos))


class TestShortRuns(unittest.TestCase):
    def test_run_shorter_than_one_frame_still_yields_a_frame(self):
        # A crashed run must still render something rather than divide by zero.
        frames = demo_lib.frame_schedule("full", start=0.0, end=0.5)
        self.assertGreaterEqual(len(frames), 1)

    def test_highlight_of_a_short_run_does_not_exceed_the_run(self):
        frames = demo_lib.frame_schedule("highlight", start=0.0, end=30.0)
        self.assertLessEqual(frames[-1][1] / 1000.0, 30.0 + 1e-6)
        self.assertGreaterEqual(len(frames), 1)


class TestVideoSeconds(unittest.TestCase):
    def test_full_video_seconds_equals_run_seconds(self):
        self.assertAlmostEqual(demo_lib.video_seconds("full", 8 * 3600.0), 8 * 3600.0,
                               delta=2.0)

    def test_highlight_video_seconds_is_the_target(self):
        self.assertAlmostEqual(demo_lib.video_seconds("highlight", 8 * 3600.0),
                               demo_lib.HIGHLIGHT_TARGET_SECONDS, delta=2.0)


if __name__ == "__main__":
    unittest.main()
