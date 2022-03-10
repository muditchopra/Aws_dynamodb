from pprint import pprint
import boto3
from botocore.exceptions import ClientError

class Employes():

    def create_employe_table(self,dynamodb=None):
        if not dynamodb:
            self.dynamodb = boto3.resource('dynamodb')
        else:
            self.dynamodb = dynamodb

        self.table = self.dynamodb.create_table(
            TableName='Employes',
            KeySchema=[
                {
                    'AttributeName': 'emp_id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'emp_id',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )

        # Wait until the table exists.
        self.table.meta.client.get_waiter('table_exists').wait(TableName='Employes')
        assert self.table.table_status == 'ACTIVE'

        return self.table

    def put_employe(self,emp_id, name, company, dynamodb=None):
        if not dynamodb:
            self.dynamodb = boto3.resource('dynamodb')
        else:
            self.dynamodb = dynamodb

        self.table = self.dynamodb.Table('Employes')
        self.response = self.table.put_item(
            Item={
                'emp_id': emp_id,
                'name': name,
                'company': company
            }
        )
        return self.response

    def get_employe(self,emp_id, dynamodb=None):
        if not dynamodb:
            self.dynamodb = boto3.resource('dynamodb')
        else:
            self.dynamodb = dynamodb

        self.table = self.dynamodb.Table('Employes')

        try:
            self.response = self.table.get_item(Key={'emp_id': emp_id})
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return self.response['Item']

    def update_employe(self,emp_id, skill, table_name, dynamodb=None):
        if not dynamodb:
            self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        else:
            self.dynamodb = dynamodb

        self.table = self.dynamodb.Table(table_name)

        try:
            self.response = self.table.update_item(
                Key={'emp_id': emp_id},
                UpdateExpression='set skill=:s',
                ExpressionAttributeValues={':s': skill},
                ReturnValues="UPDATED_NEW"
            )

        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return self.response

    def describe_employe_table(self,table_name, dynamodb=None):
        if not dynamodb:
            self.dynamodb = boto3.resource('dynamodb')
        else:
            self.dynamodb = dynamodb

        try:
            self.table = self.dynamodb.Table(table_name)
            self.result = self.table.meta.client.describe_table(TableName=table_name)
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return self.result

    def delete_employe(self,emp_id, skill, table_name, dynamodb=None):
        if not dynamodb:
            self.dynamodb = boto3.resource('dynampdb')
        else:
            self.dynamodb = dynamodb

        self.table = self.dynamodb.Table(table_name)

        try:
            self.response = self.table.delete_item(
                Key={'emp_id': emp_id},
                ConditionExpression='skill =:s',
                ExpressionAttributeValues={':s': skill}
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(e.response['Error']['Message'])
            else:
                raise
        else:
            return self.response


if __name__ == '__main__':

    obj = Employes()

    movie_table = obj.create_employe_table()
    print("Table status:", movie_table.table_status)

    movie_resp = obj.put_employe(1234, 'Honey',
                             "Infosys")
    print("Put employe succeeded:")
    pprint(movie_resp, sort_dicts=False)

    movie = obj.get_employe(1234)
    if movie:
        print("Get employe succeeded:")
        pprint(movie, sort_dicts=False)

    result = obj.update_employe(1234, ['Big data', 'AWS', 'Python'], 'Empolyes')
    pprint(result)

    response = obj.describe_employe_table('Employes')
    pprint(response)

    result = obj.delete_employe(1234, 'python', 'Employes')
    pprint(result)
