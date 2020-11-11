import unittest
import src

class TestConnection(unittest.TestCase):
    def test_create_connection(self):
        assert(src.create_connection())

class TestUuidFromCaption(unittest.TestCase):
    def setUp(self):
        self.connection = src.create_connection()
    def test_uuid_from_caption(self):
        assert(src.uuid_from_caption("COH_001",self.connection) == "96e96833-5ba0-4105-ae7c-fc4dc6a11e46")
    def test_uuid_from_caption_invalid(self):
        self.assertRaises(Exception,src.uuid_from_caption)

if __name__ == '__main__':
    unittest.main()