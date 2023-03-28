import os
import urllib
import urllib.request
import boto3
import io
import datetime

import json


separator = ";"
newline = "\n"
host = os.environ['HOST']

def get_contracts(client):
    response = client.get(f"{host}/api/contracts")
    contracts = response.json()['contracts']
    contracts_df = pd.DataFrame(contracts)
    return contracts_df
    
def get_contract_id(df, contract_number: str) -> str:
    contract_number = str(contract_number)
    if contract_number == '':
        raise Exception(f"Contracts: we need contract number {contract_number}")
    else:
        df = df.query("contractNumber == @contract_number")
        if len(df['id']) == 1:
            return df['id'].item()
        elif len(df['id']) > 1:
            raise Exception(f"Contracts: Found more than one match for contract number {contract_number}")
        else:
            raise Exception(f"Contracts: No match for contract number {contract_number}")

# Clean and format the original dataset
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Cleaning data file...", end='')
    df['DESCRIPTION'] = df['DESCRIPTION'].astype(str)
    print("OK")
    return df


# Get exceptions grouped by booking number , concatenating exception ref. numbers
def get_order_data_from_url(client) -> dict:
    order_data = pd.DataFrame()
    try:
        response = client.get(f"{host}/api/orders")
        data = json.loads(response.content)

        if data.get('orders', None) is None:
            raise ValueError("No orders found")
        elif len(data.get('disputes')) == 0:
            raise ValueError("No orders found")

        order_data = pd.json_normalize(data.get('orders'))

        print("OK")
    except Exception as error:
        print(error)
        exit(1)
    
    order_data = order_data.rename(
        columns={
            'id'                : 'ID',
            'contractId'        : 'CONTRACT_ID',
            'reference'         : 'REFERENCE'
        }
    )

    return order_data


def create_order(df, client, contracts):
    for index,item in df.iterrows():
        print(item)
        try:
            contract_id = get_contract_id(contracts,  str(item['CONTRACT_NUMBER']))
            order = {
                "contractId": contract_id,
                "description": str(item['DESCRIPTION'])
            }
            r = client.post(f"{host}/api/orders", order)
            order_ref = get_reference(r.content)
            print(f"Created item for {item['CONTRACT_NUMBER']} with a reference : {order_ref}")
        except Exception as e:
            print(f"Error creating order for {item['CONTRACT_NUMBER']}: {repr(e)}")
            continue
 

def send_text_response(bot_token, event, response_text):
    print("Messaging Slack...")
    SLACK_URL = "https://slack.com/api/chat.postMessage"
    channel_id = event.get("event").get("channel")
    
    data = urllib.parse.urlencode(
        (
            ("token", bot_token),
            ("channel", channel_id),
            ("text", response_text)
        )
    )
    
    data = data.encode("ascii")
    request = urllib.request.Request(SLACK_URL, data=data, method="POST")
    request.add_header( "Content-Type", "application/x-www-form-urlencoded" )
    x = urllib.request.urlopen(request).read()

# Main
def lambda_handler(event, context):
    print(os.environ['ENV'])
    if os.environ.get('ENV') == 'dev':
        print("Running in dev mode")
        session = get_sts_session()
    else:
        session = boto3.session.Session()

    s3_resource = boto3.resource("s3")
    secret_name = os.environ['SECRET']
    region = os.environ['REGION']
    
    os.environ['BOT_TOKEN'] = json.loads(get_secret('slack_bot_token', 'us-east-1', session=session)).get('BOT_TOKEN')
    bot_token = os.environ['BOT_TOKEN']

    send_text_response(bot_token, event.get('detail'), "Create Exceptions? On it!")
    try: 
        
        if event.get('detail').get('event').get('files', None) is None:
            send_text_response(bot_token, event.get('detail'), "No files in message")
            return "200 OK"
        
        for file in event.get('detail').get('event').get('files'):
            file_url = file.get('url_private_download')
            file_name = file.get('name')

            file_obj = {
                    "filename": file_name,
                } 

        # Download file from Slack
        try:
            request = urllib.request.Request(file_url)
            request.add_header( "Authorization", f"Bearer {bot_token}" )

            x = urllib.request.urlopen(request)        
            b_buf = io.BytesIO(x.read())
            b_buf.seek(0)

            file_obj['contents'] = b_buf

        except Exception as e:
            send_text_response(bot_token, event.get('detail'), f"Error downloading file from Slack: {e}")
            file_obj['status'] = 'error' 
        
        dtypes = {
            "CONTRACT_NUMBER": object,
            "DESCRIPTION": object
        }

        daily_log = pd.read_csv(
            file_obj['contents'], 
            header=0,
            dtype=dtypes, 
            error_bad_lines=False)

        daily_log = clean_data(daily_log).drop(columns=['EXCEPTION_REFERENCE'])

        order_data = get_order_data_from_url(client)
        print(daily_log.dtypes)
        daily_log = pd.merge(daily_log, order_data, how='left', left_on=['CONTRACT_NUMBER'], right_on=['CONTRACT_NUMBER'])

        print("OK")

        contracts = get_contracts(client)
        print(contracts.shape)
        print(contracts.columns)


        # Upload to S3
        curr_date = datetime.today()
        bucket_name = os.environ['BUCKET']        
        try: 
            bucket = s3_resource.Bucket(bucket_name)
            # bucket.upload_fileobj(daily_log.astype(str), key)
            r = pandas_df_to_s3_csv(data=daily_log, bucket=bucket_name, path=key, session=session)
            if r:
                print("YAY!")
                
            url = boto3.client('s3', region_name='us-west-2').generate_presigned_url(
                ClientMethod='get_object', 
                Params={'Bucket': bucket_name, 'Key': key},
                ExpiresIn=3600
            )
            
            send_text_response(bot_token, event.get('detail'), f"Orders processed! Results can be found at the following url!")
            send_text_response(bot_token, event.get('detail'), f"{url}")
        except Exception as e:
            send_text_response(bot_token, event.get('detail'), f"Error uploading file to S3: {e}")
    except Exception as e:
        send_text_response(bot_token, event.get('detail'), f"Error: {e}")
        return f"I've encountered an error... {e}"

if __name__ == "__main__":
    context = 0
    with open("./test-data.json", "r") as f:
        event = json.load(f)
    
    lambda_handler(event, context)
