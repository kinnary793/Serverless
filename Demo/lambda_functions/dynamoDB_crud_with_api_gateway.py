import json
import boto3

def lambda_handler(event, context):
    # CURD operation for dynamodb table
    route = event['resource']
    # create resource for dynamodb
    dynamodb = boto3.resource('dynamodb')
    
    # get the specified table details from dynamodb
    tableName = event['queryStringParameters']['tableName']
    id = event['pathParameters']['id']
    table = dynamodb.Table(tableName)
    
    item = json.loads(event['body'])
    
    # Update item from the table
    if route == '/updateitem/{id}':
        updateItem(table, id, item)
        
    # Put item into the table
    elif route == '/additem':
        addItem(table, item)
        
    # Get items from the table
    elif route == '/items':
        getItems(table)
        
    # Get item from the table
    elif route == '/items/{id}':
        getItem(table, id) 
        
    # Delete item from the table
    elif route == '/deleteitem/{id}':
        deleteItem(table, id)
    
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('400 Bad Request')
        }

def addItem(table, item):
    table.put_item(
        Item = item
    )
    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/json"
        },
        'body': json.dumps('Item Added Successfully!')
    }

def updateItem(table, id, item):
    expression = 'SET '
    values = {}
    names = {}
    icm = 1
    
    for x,y in item.items():
        print(x,y)
        expression += f"#x{icm} = :y{icm},"
        values.__setitem__(f":y{icm}", y)
        names.__setitem__(f"#x{icm}", x)
        icm += 1
    expression = expression[:len(expression)-1]
    
    table.update_item(
        Key = {
            'id': id
            },
        UpdateExpression = expression,
        ExpressionAttributeValues = values,
        ExpressionAttributeNames = names
    )
    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/json"
        },
        'body': json.dumps('Item Updated Successfully!')
    }

def getItem(table, id):
    response = table.get_item(
    Key = {
        'id': id
        }
    )
    return {
        'statusCode': 200,
        'body': json.dumps(response['Item'])
    }


def getItems(table):
    # Using scan we're fetching the data
    resp = table.scan()
    
    print("Table Details:\n",resp)
    print("Number of Items are there in table:",resp['Count'])
    print("table-items:",resp['Items'])
    return {
        'statusCode': 200,
        'body': json.dumps(resp['Items'])
    }

def deleteItem(table,id):
    table.delete_item(
        Key = {
            'id': id
        }
    )
    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/json"
        },
        'body': json.dumps('Item Deleted Successfully!')
    }
