import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["INGEST_BUCKET"]

    # Generar UUID para el comentario
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }

    # Guardar el comentario en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)

    # Subir el JSON del comentario al bucket S3
    s3 = boto3.client('s3')
    archivo_s3 = f"{tenant_id}/{uuidv1}.json"
    s3.put_object(
        Bucket=nombre_bucket,
        Key=archivo_s3,
        Body=json.dumps(comentario),
        ContentType='application/json'
    )

    # Salida (json)
    print(f"Comentario guardado en S3: {archivo_s3}")
    return {
        'statusCode': 200,
        'comentario': comentario,
        'response': response
    }
