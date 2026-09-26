import os
import unittest
from app.server import app
from app.backend.auth import authenticate_user


class TestWebAppAuthAndApi(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.admin_user = os.getenv("ADMIN_USERNAME", "admin")
        self.admin_pass = os.getenv("ADMIN_PASSWORD", "admin123")
        self.viewer_user = os.getenv("VIEWER_USERNAME", "visitor")
        self.viewer_pass = os.getenv("VIEWER_PASSWORD", "visitor123")

    def test_direct_auth_logic(self):
        # Admin authentication
        admin_res = authenticate_user(self.admin_user, self.admin_pass)
        self.assertIsNotNone(admin_res)
        self.assertEqual(admin_res["role"], "ADMIN")

        # Viewer authentication
        viewer_res = authenticate_user(self.viewer_user, self.viewer_pass)
        self.assertIsNotNone(viewer_res)
        self.assertEqual(viewer_res["role"], "VIEWER")

        # Invalid credentials
        self.assertIsNone(authenticate_user("admin", "invalid_password"))
        self.assertIsNone(authenticate_user("unknown_user", "some_pass"))
        self.assertIsNone(authenticate_user("", ""))

    def test_protected_endpoints_require_login(self):
        # Unauthenticated access should return 401
        res = self.client.get("/api/stats")
        self.assertEqual(res.status_code, 401)

        res = self.client.get("/api/analytics/kpis")
        self.assertEqual(res.status_code, 401)

    def test_admin_flow_and_analytics_endpoints(self):
        # Login
        login_res = self.client.post("/api/login", json={
            "username": self.admin_user,
            "password": self.admin_pass
        })
        self.assertEqual(login_res.status_code, 200)
        self.assertEqual(login_res.get_json()["role"], "ADMIN")

        # Session verification
        me_res = self.client.get("/api/me")
        self.assertEqual(me_res.status_code, 200)
        self.assertTrue(me_res.get_json()["logged_in"])

        # Analytics KPIs
        kpis_res = self.client.get("/api/analytics/kpis")
        self.assertEqual(kpis_res.status_code, 200)
        self.assertIn("total_games", kpis_res.get_json())

        # Analytics Genres
        genres_res = self.client.get("/api/analytics/genres?limit=5")
        self.assertEqual(genres_res.status_code, 200)
        self.assertIsInstance(genres_res.get_json(), list)

        # Analytics Top Games
        top_res = self.client.get("/api/analytics/top-games?limit=5")
        self.assertEqual(top_res.status_code, 200)
        self.assertIsInstance(top_res.get_json(), list)

        # Analytics Timeline
        timeline_res = self.client.get("/api/analytics/timeline")
        self.assertEqual(timeline_res.status_code, 200)
        self.assertIsInstance(timeline_res.get_json(), list)

        # Games Explorer
        games_res = self.client.get("/api/analytics/games?limit=5")
        self.assertEqual(games_res.status_code, 200)
        self.assertIn("items", games_res.get_json())

        # Logout
        logout_res = self.client.post("/api/logout")
        self.assertEqual(logout_res.status_code, 200)

        me_after = self.client.get("/api/me")
        self.assertFalse(me_after.get_json()["logged_in"])

    def test_viewer_rbac_restrictions(self):
        # Login as viewer
        login_res = self.client.post("/api/login", json={
            "username": self.viewer_user,
            "password": self.viewer_pass
        })
        self.assertEqual(login_res.status_code, 200)
        self.assertEqual(login_res.get_json()["role"], "VIEWER")

        # Viewer can read analytics
        kpis_res = self.client.get("/api/analytics/kpis")
        self.assertEqual(kpis_res.status_code, 200)

        # Viewer is blocked from Admin operations (403 Forbidden)
        override_res = self.client.post("/api/checkpoints/override", json={
            "table_name": "GameSchema",
            "activate_fallback": True
        })
        self.assertEqual(override_res.status_code, 403)

        fallback_res = self.client.post("/api/fallback-events", json={
            "table_name": "GameSchema",
            "start_watermark": 1000,
            "end_watermark": 2000
        })
        self.assertEqual(fallback_res.status_code, 403)


if __name__ == "__main__":
    unittest.main()
