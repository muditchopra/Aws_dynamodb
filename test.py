from pprint import pprint
import unittest
import boto3
from botocore.exceptions import ClientError
from moto import mock_dynamodb2
from Employes import Employes

emp_obj = Employes()

@mock_dynamodb2
class TestDatabaseFunctions(unittest.TestCase):
    def setUp(self):
        """Create the mock database and table"""
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

        self.table = emp_obj.create_employe_table(self.dynamodb)

    def tearDown(self):
        """Delete mock database and table after test is run"""
        self.table.delete()
        self.dynamodb = None

    def test_table_exists(self):
        self.assertTrue(self.table)  # check if we got a result
        self.assertIn('Employes', self.table.name)  # check if the table name is 'Movies'
        # pprint(self.table.name)

    def test_put_employe(self):

        self.result = emp_obj.put_employe(1234, 'Honey',
                           "Infosys", self.dynamodb)

        self.assertEqual(200, self.result['ResponseMetadata']['HTTPStatusCode'])
        # pprint(self.result, sort_dicts=False)


    def test_get_employe(self):

        emp_obj.put_employe(1234, 'Honey',
                           "Infosys", self.dynamodb)
        self.result = emp_obj.get_employe(1234, self.dynamodb)

        self.assertEqual(1234, self.result['emp_id'])
        self.assertEqual("Honey", self.result['name'])
        self.assertEqual("Infosys", self.result['company'])
        # pprint(self.result, sort_dicts=False)

    def test_describe_table(self):

        self.result = emp_obj.describe_employe_table('Employes',self.dynamodb)
        self.assertEqual(200,self.result['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('ACTIVE',self.result['Table']['TableStatus'])
        # pprint(self.result['Table'])

    def test_update_details(self):
        emp_obj.put_employe(1234, 'Honey',
                            "Infosys", self.dynamodb)
        self.result = emp_obj.update_employe(1234,['Big data','AWS','Python'],'Employes', self.dynamodb)
        self.assertEqual(200, self.result['ResponseMetadata']['HTTPStatusCode'])
        # pprint(self.result['Attributes'])

    def test_delete_employe(self):
        emp_obj.put_employe(1234, 'Honey',
                            "Infosys", self.dynamodb)
        self.result = emp_obj.delete_employe(1234,'Python','Employe',self.dynamodb)
        #pprint(self.result)

if __name__ == '__main__':
    unittest.main()