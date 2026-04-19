import unittest

from app import app


class SmokeTests(unittest.TestCase):
    def setUp(self):
        app.config["TESTING"] = True
        self.client = app.test_client()

    def test_public_pages_load(self):
        for path in ("/", "/track", "/login", "/api/public-commuter"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertLess(response.status_code, 500)

    def test_admin_redirects_to_login(self):
        response = self.client.get("/admin")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_login_requires_csrf(self):
        response = self.client.post("/login", data={"username": "admin", "password": "admin123"})
        self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
