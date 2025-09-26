import unittest
from app.services import FacilityServices, EncounterServices, DiseaseCategoryServices, DiseaseServices
from app.services import BaseServices, UserServices
from app.exceptions import DuplicateError, InvalidReferenceError, MissingError, ValidationError, AuthenticationError
from app.models import Facility, Encounter, DiseaseCategory, Disease, User
from datetime import datetime
from app import app
from app.db import get_db, close_db
import sqlite3

class BaseServicesTestCase(unittest.TestCase):

    def setUp(self):
        self.app = app
        self.app_context = app.app_context()
        self.app_context.push()
        self.app.config['TESTING'] = True
        self.app.config['DATABASE'] = ':memory:'
        db = get_db()
        with self.app.open_resource('schema.sql') as f:
            db.executescript(f.read().decode('utf8'))
        db.commit()
        db = get_db()
        cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]

    def tearDown(self):
        close_db()
        self.app_context.pop()
    
    def test_database(self):
        db = get_db()
        cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        self.assertIn('facility', tables)
        self.assertIn('users', tables)
        self.assertIn('diseases_category', tables)
        self.assertIn('diseases', tables)
        self.assertIn('encounters', tables)

# ------------------- Facility Tests -------------------
class FacilityServicesTestCase(BaseServicesTestCase):
    def setUp(self):
        super().setUp()
        # Create a default facility for tests
        FacilityServices.create_facility("TestFacility1", "Owo", "Primary")

    def test_create_and_get_facility(self):
        facility = FacilityServices.get_facility_by_name("TestFacility1")
        self.assertIsInstance(facility, Facility)
        self.assertEqual(facility.name, "TestFacility1")
        self.assertEqual(facility.local_government, "Owo")
        self.assertEqual(facility.facility_type, "Primary")
        fetched_facility = FacilityServices.get_facility_by_name("TestFacility1")
        self.assertEqual(facility, fetched_facility)
        self.assertEqual(facility.id, fetched_facility.id)

    def test_create_duplicate_facility(self):
        FacilityServices.create_facility("TestFacility2", "Akure South", "Secondary")
        with self.assertRaises(DuplicateError):
            FacilityServices.create_facility("TestFacility2", "Akure North", "Secondary")

    def test_update_facility(self):
        facility = FacilityServices.get_facility_by_name("TestFacility1")
        facility.local_government = "Ondo West"
        FacilityServices.update_facility(facility)
        updated_facility = FacilityServices.get_facility_by_name("TestFacility1")
        self.assertEqual(updated_facility.local_government, "Ondo West")
        self.assertEqual(updated_facility.name, "TestFacility1")
        self.assertEqual(updated_facility.facility_type, "Primary")
        self.assertEqual(updated_facility.id, facility.id)

    def test_get_nonexistent_facility(self):
        with self.assertRaises(MissingError):
            FacilityServices.get_facility_by_name("NonExistentFacility")

    def test_delete_facility(self):
        facility = FacilityServices.create_facility("TestFacility3", "Okitipupa", "Tertiary")
        FacilityServices.delete_facility(facility)
        with self.assertRaises(MissingError):
            FacilityServices.get_facility_by_name("TestFacility3")


class UserServicesTestCase(BaseServicesTestCase):
    def setUp(self):
        super().setUp()
        # Create facilities required for users
        FacilityServices.create_facility("TestFacility1", "Owo", "Primary")
        FacilityServices.create_facility("TestFacility2", "Akure South", "Secondary")

    def test_create_and_get_user(self):
        user1 = UserServices.create_user("user1", 1, "damilare20")
        self.assertIsInstance(user1, User)
        self.assertEqual(user1.id, 1)
        self.assertEqual(user1.username, "user1")
        self.assertEqual(user1.facility_id, 1)

        user2 = UserServices.create_user("user2", 2, "damilare20")
        self.assertIsInstance(user2, User)
        self.assertEqual(user2.id, 2)
        self.assertEqual(user2.username, "user2")
        self.assertEqual(user2.facility_id, 2)

        user1 = UserServices.get_user_by_id(1)
        user11 = UserServices.get_user_by_username("user1")
        self.assertEqual(user1.username, "user1")
        self.assertEqual(user11.username, user1.username)
        self.assertEqual(user1.facility_id, 1)
        self.assertEqual(user11.facility_id, user1.facility_id)

        with self.assertRaises(MissingError):
            UserServices.get_user_by_id(10)
        with self.assertRaises(MissingError):
            UserServices.get_user_by_username("NonExistentUser")  

    def test_authenticated_user(self):
        user = UserServices.create_user("user1", 1, "damilare20")
        user1 = UserServices.get_user_by_id(1)
        with self.assertRaises(AuthenticationError):
            UserServices.get_verified_user("user1", "wrongpassword")

        user_verified = UserServices.get_verified_user("user1", "damilare20")
        self.assertEqual(user_verified.username, user1.username)
        self.assertEqual(user_verified.facility_id, user1.facility_id)
        self.assertEqual(user_verified.password_hash, user1.password_hash)

    def test_duplicate_user(self):
        user = UserServices.create_user("user1", 1, "damilare20")
        with self.assertRaises(DuplicateError):
            UserServices.create_user("user1", 1, "damilare20")

    def test_case_insensitive_username(self):
        UserServices.create_user("user1", 1, "damilare20")
        with self.assertRaises(DuplicateError):
            UserServices.create_user("USER1", 1, "damilare20")

    def test_update_user_password(self):
        user = UserServices.create_user("user1", 1, "damilare20")
        UserServices.update_user_password(user, "newpassword")
        updated_user = UserServices.get_verified_user("user1", "newpassword")
        self.assertEqual(updated_user.username, "user1")
        with self.assertRaises(AuthenticationError):
            UserServices.get_verified_user("user1", "damilare20")

    def test_reference_error(self):
        with self.assertRaises(InvalidReferenceError):
            UserServices.create_user("user3", 999, "damilare20")

    def test_update_user(self):
        user = UserServices.create_user("user1", 1, "damilare20")
        user1 = UserServices.get_user_by_id(1)
        user1.username = "updated_user1"
        UserServices.update_user_details(user1)
        updated_user1 = UserServices.get_user_by_id(1)
        self.assertEqual(updated_user1.username, "updated_user1")
        self.assertEqual(updated_user1.facility_id, 1)
        self.assertEqual(updated_user1.password_hash, user1.password_hash)
        self.assertEqual(updated_user1.id, user1.id)

    def test_delete_user(self):
        user2 = UserServices.create_user("user2", 2, "damilare20")
        UserServices.delete_user(user2)
        with self.assertRaises(MissingError):
            UserServices.get_user_by_id(user2.id)

class DiseaseCategoryServicesTestCase(BaseServicesTestCase):
    def test_create_and_get_disease_category(self):
        category = DiseaseCategoryServices.create_category("Infectious Diseases")
        self.assertIsInstance(category, DiseaseCategory)
        self.assertEqual(category.category_name, "Infectious Diseases")
        fetched_category = DiseaseCategoryServices.get_category_by_name("Infectious Diseases")
        self.assertEqual(category, fetched_category)
        self.assertEqual(category.id, fetched_category.id)

    def test_create_duplicate_disease_category(self):
        DiseaseCategoryServices.create_category("Chronic Diseases")
        with self.assertRaises(DuplicateError):
            DiseaseCategoryServices.create_category("Chronic Diseases")

    def test_get_nonexistent_disease_category(self):
        with self.assertRaises(MissingError):
            DiseaseCategoryServices.get_category_by_name("NonExistentCategory")

class DiseaseServicesTestCase(BaseServicesTestCase):
    def setUp(self):
        super().setUp()
        self.category = DiseaseCategoryServices.create_category("Infectious Diseases")
        self.category2 = DiseaseCategoryServices.create_category("Chronic Diseases")

    def test_create_and_get_disease(self):
        disease = DiseaseServices.create_disease("Malaria", self.category.id)
        self.assertIsInstance(disease, Disease)
        self.assertEqual(disease.name, "Malaria")
        self.assertEqual(disease.category_id, self.category.id)
        fetched_disease = DiseaseServices.get_disease_by_name("Malaria")
        self.assertEqual(disease, fetched_disease)
        self.assertEqual(disease.id, fetched_disease.id)

    def test_create_duplicate_disease(self):
        DiseaseServices.create_disease("Typhoid", self.category.id)
        with self.assertRaises(DuplicateError):
            DiseaseServices.create_disease("Typhoid", self.category.id)

    def test_update_disease(self):
        disease = DiseaseServices.create_disease("Hypertension", self.category2.id)
        disease.name = "High Blood Pressure"
        DiseaseServices.update_disease(disease)
        updated_disease = DiseaseServices.get_disease_by_name("High Blood Pressure")
        self.assertEqual(updated_disease.name, "High Blood Pressure")
        self.assertEqual(updated_disease.category_id, self.category2.id)
        self.assertEqual(updated_disease.id, disease.id)

    def test_create_disease_invalid_category(self):
        with self.assertRaises(InvalidReferenceError):
            DiseaseServices.create_disease("Ebola", 999)

    def test_get_nonexistent_disease(self):
        with self.assertRaises(MissingError):
            DiseaseServices.get_disease_by_name("NonExistentDisease")


class EncounterServicesTestCase(BaseServicesTestCase):
    def setUp(self):
        super().setUp()
        FacilityServices.create_facility("TestFacility1", "Owo", "Primary")
        self.category = DiseaseCategoryServices.create_category("Infectious Diseases")
        self.disease = DiseaseServices.create_disease("Malaria", self.category.id)
        self.user = UserServices.create_user("user1", 1, "damilare20")

    def test_create_and_get_encounter(self):
        encounter = EncounterServices.create_encounter(
            facility_id=1,
            disease_id=self.disease.id,
            date=datetime.now().date(),
            policy_number="ABC/123/456/X/0",
            client_name="John Doe",
            gender="M",
            age=30,
            treatment="Antibiotics",
            referral=True,
            doctor_name="Dr. Smith",
            professional_service="Consultation",
            created_by=self.user.id
        )
        self.assertIsInstance(encounter, Encounter)
        self.assertEqual(encounter.client_name, "John Doe")
        self.assertEqual(encounter.age_group, "20-44")
        fetched = EncounterServices.get_encounter_by_id(encounter.id)
        self.assertEqual(encounter, fetched)

    def test_invalid_policy_number(self):
        with self.assertRaises(ValidationError):
            EncounterServices.create_encounter(
                facility_id=1,
                disease_id=self.disease.id,
                date=datetime.now().date(),
                policy_number="INVALID",
                client_name="John Doe",
                gender="M",
                age=30,
                treatment="Antibiotics",
                referral=True,
                doctor_name="Dr. Smith",
                professional_service="Consultation",
                created_by=self.user.id
            )

    def test_get_encounter_by_facility(self):
        EncounterServices.create_encounter(
            facility_id=1,
            disease_id=self.disease.id,
            date=datetime.now().date(),
            policy_number="ABC/123/456/X/0",
            client_name="John Doe",
            gender="M",
            age=30,
            treatment="Antibiotics",
            referral=True,
            doctor_name="Dr. Smith",
            professional_service="Consultation",
            created_by=self.user.id
        )
        encounters = list(EncounterServices.get_encounter_by_facility("TestFacility1", 1))
        self.assertEqual(len(encounters), 1)
        self.assertEqual(encounters[0].client_name, "John Doe")

    def test_invalid_gender(self):
        with self.assertRaises(ValidationError):
            EncounterServices.create_encounter(
                facility_id=1,
                disease_id=self.disease.id,
                date=datetime.now(),
                policy_number="ABC/123/456/X/0",
                client_name="John Doe",
                gender="X",  # Invalid
                age=30,
                treatment="Antibiotics",
                referral=True,
                doctor_name="Dr. Smith",
                professional_service="Consultation",
                created_by=self.user.id
            )


if __name__ == '__main__':
    unittest.main()
