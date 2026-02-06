import unittest
from app.services import FacilityServices, EncounterServices, DiseaseCategoryServices, DiseaseServices
from app.services import BaseServices, UserServices, InsuranceSchemeServices, TreatmentOutcomeServices
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
        # Create a default insurance scheme for tests
        self.scheme = InsuranceSchemeServices.create_scheme("TestScheme", "#000000")

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
        FacilityServices.create_facility("TestFacility1", "Owo", "Primary", scheme=[self.scheme.id])

    def test_create_and_get_facility(self):
        facility = FacilityServices.get_facility_by_name("TestFacility1")
        facility2 = FacilityServices.get_by_id(1)
        self.assertIsInstance(facility, Facility)
        self.assertIsInstance(facility2, Facility)
        self.assertEqual(facility.name, "TestFacility1")
        self.assertEqual(facility2.name, "TestFacility1")
        self.assertEqual(facility.local_government, "Owo")
        self.assertEqual(facility2.local_government, "Owo")
        self.assertEqual(facility.facility_type, "Primary")
        self.assertEqual(facility2.facility_type, "Primary")

    def test_create_duplicate_facility(self):
        FacilityServices.create_facility("TestFacility2", "Akure South", "Secondary", scheme=[self.scheme.id])
        with self.assertRaises(DuplicateError):
            FacilityServices.create_facility("TestFacility2", "Akure North", "Secondary", scheme=[self.scheme.id])

    def test_update_facility(self):
        facility = FacilityServices.get_facility_by_name("TestFacility1")
        facility.local_government = "Ondo West"
        FacilityServices.update_facility(facility, scheme=[self.scheme.id])
        updated_facility = FacilityServices.get_facility_by_name("TestFacility1")
        self.assertEqual(updated_facility.local_government, "Ondo West")
        self.assertEqual(updated_facility.name, "TestFacility1")
        self.assertEqual(updated_facility.facility_type, "Primary")
        self.assertEqual(updated_facility.id, facility.id)

    def test_get_nonexistent_facility(self):
        with self.assertRaises(MissingError):
            FacilityServices.get_facility_by_name("NonExistentFacility")

    def test_delete_facility(self):
        facility = FacilityServices.create_facility("TestFacility3", "Okitipupa", "Secondary", scheme=[self.scheme.id])
        FacilityServices.delete_facility(facility)
        with self.assertRaises(MissingError):
            FacilityServices.get_facility_by_name("TestFacility3")

    def test_facility_get_all(self):
        res = list(FacilityServices.get_all())
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].name, 'TestFacility1')

class UserServicesTestCase(BaseServicesTestCase):
    def setUp(self):
        super().setUp()
        # Create facilities required for users
        FacilityServices.create_facility("TestFacility1", "Owo", "Primary", scheme=[self.scheme.id])
        FacilityServices.create_facility("TestFacility2", "Akure South", "Secondary", scheme=[self.scheme.id])

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

        user1 = UserServices.get_by_id(1)
        user11 = UserServices.get_user_by_username("user1")
        self.assertEqual(user1.username, "user1")
        self.assertEqual(user11.username, user1.username)
        self.assertEqual(user1.facility_id, 1)
        self.assertEqual(user11.facility_id, user1.facility_id)

        with self.assertRaises(MissingError):
            UserServices.get_by_id(10)
        with self.assertRaises(MissingError):
            UserServices.get_user_by_username("NonExistentUser")  

    def test_authenticated_user(self):
        user = UserServices.create_user("user1", 1, "damilare20")
        user1 = UserServices.get_by_id(1)
        with self.assertRaises(AuthenticationError):
            UserServices.get_verified_user("user1", "wrongpassword")

        user_verified = UserServices.get_verified_user("user1", "damilare20")
        self.assertEqual(user_verified.username, user1.username)
        # UserView now has facility (FacilityView) instead of facility_id
        self.assertEqual(user_verified.facility.id, user1.facility_id)

    def test_get_user_all(self):
        from app.models import UserView, FacilityView
        from app.filter_parser import Params
        user = UserServices.create_user("user1", 1, "damilare20")
        user = UserServices.create_user("user2", 2, "damilare20")
        user = UserServices.create_user("user3", 2, "damilare20")
        user = UserServices.create_user("user4", 1, "damilare20")
        res = list(UserServices.get_all())
        users = {'user1', 'user2', 'user3', 'user4'}
        user_found = set()
        self.assertEqual(len(res), 4)
        for elem in res:
            self.assertIsInstance(elem, UserView)
            self.assertIn(elem.username, users)
            self.assertIsInstance(elem.facility, FacilityView)
            self.assertNotIn(elem.username, user_found)
            user_found.add(elem.username)
        self.assertEqual(UserServices.get_total(), 4)
        # Use Params with limit to restrict results
        params = Params().set_limit(1)
        res = list(UserServices.list_row_by_page(page=1, params=params))
        self.assertEqual(len(res), 1)

    def test_get_all_with_filter(self):
        user = UserServices.create_user("user1", 1, "damilare20")
        print("Testing get all with filter")
        res = list(UserServices.get_all(and_filter =[('username', 'user1', '=')]))
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].username, 'user1')
        self.assertEqual(res[0].id, 1)


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
        user1 = UserServices.get_by_id(1)
        user1.username = "updated_user1"
        UserServices.update_user(user1)
        updated_user1 = UserServices.get_by_id(1)
        self.assertEqual(updated_user1.username, "updated_user1")
        self.assertEqual(updated_user1.facility_id, 1)
        self.assertEqual(updated_user1.password_hash, user1.password_hash)
        self.assertEqual(updated_user1.id, user1.id)

    def test_delete_user(self):
        user2 = UserServices.create_user("user2", 2, "damilare20")
        UserServices.delete_user(user2)
        with self.assertRaises(MissingError):
            UserServices.get_by_id(user2.id)

class DiseaseCategoryServicesTestCase(BaseServicesTestCase):
    def test_create_and_get_disease_category(self):
        category = DiseaseCategoryServices.create_category("Infectious Diseases")
        self.assertIsInstance(category, DiseaseCategory)
        self.assertEqual(category.category_name, "Infectious Diseases")
        fetched_category = DiseaseCategoryServices.get_by_id(1)
        self.assertEqual(category, fetched_category)
        self.assertEqual(category.id, fetched_category.id)

    def test_create_duplicate_disease_category(self):
        DiseaseCategoryServices.create_category("Chronic Diseases")
        with self.assertRaises(DuplicateError):
            DiseaseCategoryServices.create_category("Chronic Diseases")

    def test_get_nonexistent_disease_category(self):
        with self.assertRaises(MissingError):
            DiseaseCategoryServices.get_by_id(1)

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
        FacilityServices.create_facility("TestFacility1", "Owo", "Primary", scheme=[self.scheme.id])
        self.category = DiseaseCategoryServices.create_category("Infectious Diseases")
        self.disease = DiseaseServices.create_disease("Malaria", self.category.id)
        self.user = UserServices.create_user("user1", 1, "damilare20")
        # Create a treatment outcome for tests
        self.outcome = TreatmentOutcomeServices.create_treatment_outcome("Discharged", "Alive")

    def test_create_and_get_encounter(self):
        encounter = EncounterServices.create_encounter(
            facility_id=1,
            date=datetime.now().date(),
            policy_number="ABC/123/456/X/0",
            client_name="John Doe",
            gender="M",
            age=30,
            treatment="Antibiotics",
            doctor_name="Dr. Smith",
            scheme=self.scheme.id,
            nin="12345678901",
            phone_number="08012345678",
            outcome=self.outcome.id,
            created_by=self.user.id,
            diseases_id=[self.disease.id]
        )
        self.assertIsInstance(encounter, Encounter)
        self.assertEqual(encounter.client_name, "John Doe")
        self.assertEqual(encounter.age_group, "20-44")
        fetched = EncounterServices.get_by_id(encounter.id)
        self.assertEqual(encounter, fetched)

    def test_get_encounter_by_facility(self):
        EncounterServices.create_encounter(
            facility_id=1,
            date=datetime.now().date(),
            policy_number="ABC/123/456/X/0",
            client_name="John Doe",
            gender="M",
            age=30,
            treatment="Antibiotics",
            doctor_name="Dr. Smith",
            scheme=self.scheme.id,
            nin="12345678901",
            phone_number="08012345678",
            outcome=self.outcome.id,
            created_by=self.user.id,
            diseases_id=[self.disease.id]
        )
        from app.models import EncounterView
        encounters = list(EncounterServices.get_encounter_by_facility(1))
        self.assertIsInstance(encounters[0], EncounterView)
        ec: EncounterView = encounters[0]
        self.assertEqual(ec.facility.name, "TestFacility1")
        self.assertEqual(ec.created_by, "user1")
        # diseases is now a list
        self.assertEqual(len(ec.diseases), 1)
        self.assertEqual(ec.diseases[0].name, "Malaria")
        self.assertEqual(ec.diseases[0].category.category_name, "Infectious Diseases")
        self.assertEqual(ec.facility.lga, "Owo")
        self.assertEqual(len(encounters), 1)
        self.assertEqual(encounters[0].client_name, "John Doe")

    def test_invalid_gender(self):
        with self.assertRaises(ValidationError):
            EncounterServices.create_encounter(
                facility_id=1,
                date=datetime.now().date(),
                policy_number="ABC/123/456/X/0",
                client_name="John Doe",
                gender="X",  # Invalid
                age=30,
                treatment="Antibiotics",
                doctor_name="Dr. Smith",
                scheme=self.scheme.id,
                nin="12345678901",
                phone_number="08012345678",
                outcome=self.outcome.id,
                created_by=self.user.id,
                diseases_id=[self.disease.id]
            )
    
    def test_age_groups(self):
        """Test that age groups are correctly generated by database"""
        test_cases = [
            (0, '<1'),
            (1, '1-5'),
            (5, '1-5'),
            (6, '6-14'),
            (14, '6-14'),
            (15, '15-19'),
            (19, '15-19'),
            (20, '20-44'),
            (44, '20-44'),
            (45, '45-64'),
            (64, '45-64'),
            (65, '65&AB'),
            (100, '65&AB'),
        ]
        
        for age, expected_group in test_cases:
            encounter = EncounterServices.create_encounter(
                facility_id=1,
                date=datetime.now().date(),
                policy_number=f"TST/{age}/123/T/{age % 6}",
                client_name=f"Test Patient {age}",
                gender="M",
                age=age,
                treatment="Test",
                doctor_name="Dr. Test",
                scheme=self.scheme.id,
                nin="12345678901",
                phone_number="08012345678",
                outcome=self.outcome.id,
                created_by=self.user.id,
                diseases_id=[self.disease.id]
            )
            self.assertEqual(
                encounter.age_group, 
                expected_group,
                f"Age {age} should map to group '{expected_group}' but got '{encounter.age_group}'"
            )

        

    def test_gender_normalization(self):
        """Test that lowercase gender is normalized to uppercase"""
        encounter = EncounterServices.create_encounter(
            facility_id=1,
            date=datetime.now().date(),
            policy_number="ABC/123/456/X/0",
            client_name="Jane Doe",
            gender="f",  # lowercase
            age=25,
            treatment="Treatment",
            doctor_name="Dr. Jones",
            scheme=self.scheme.id,
            nin="12345678901",
            phone_number="08012345678",
            outcome=self.outcome.id,
            created_by=self.user.id,
            diseases_id=[self.disease.id]
        )
        self.assertEqual(encounter.gender, "F")    

    def test_invalid_age(self):
        """Test that invalid ages are rejected"""
        with self.assertRaises(ValidationError):
            EncounterServices.create_encounter(
                facility_id=1,
                date=datetime.now().date(),
                policy_number="ABC/123/456/X/0",
                client_name="Invalid Age",
                gender="M",
                age=-1,  # Negative age
                treatment="Treatment",
                doctor_name="Dr. Test",
                scheme=self.scheme.id,
                nin="12345678901",
                phone_number="08012345678",
                outcome=self.outcome.id,
                created_by=self.user.id,
                diseases_id=[self.disease.id]
            )
        
        with self.assertRaises(ValidationError):
            EncounterServices.create_encounter(
                facility_id=1,
                date=datetime.now().date(),
                policy_number="ABC/123/456/X/1",
                client_name="Too Old",
                gender="F",
                age=121,  # Over 120
                treatment="Treatment",
                doctor_name="Dr. Test",
                scheme=self.scheme.id,
                nin="12345678901",
                phone_number="08012345678",
                outcome=self.outcome.id,
                created_by=self.user.id,
                diseases_id=[self.disease.id]
            )

    def test_encounter_immutability(self):
        """Test that encounters cannot be updated"""
        encounter = EncounterServices.create_encounter(
            facility_id=1,
            date=datetime.now().date(),
            policy_number="ABC/123/456/X/0",
            client_name="Original Name",
            gender="M",
            age=25,
            treatment="Original Treatment",
            doctor_name="Dr. Original",
            scheme=self.scheme.id,
            nin="12345678901",
            phone_number="08012345678",
            outcome=self.outcome.id,
            created_by=self.user.id,
            diseases_id=[self.disease.id]
        )
        
        encounter.client_name = "Updated Name"
        with self.assertRaises(NotImplementedError):
            EncounterServices.update_data(encounter)


if __name__ == '__main__':
    unittest.main()
