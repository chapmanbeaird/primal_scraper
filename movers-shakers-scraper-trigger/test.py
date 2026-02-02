import boto3
import os

# Initialize the boto3 ECS client
ecs_client = boto3.client('ecs')

def handler(event, context):
    task_number = 2
    try:
        # Define ECS cluster, task definition, and network configuration
        cluster_name = 'scraper-cluster'
        task_definition = f"daily-movers-shakers-scraper:{task_number}"
        subnet_id = 'subnet-0c710cf5cf5cc2c4d'
        security_group_id = 'sg-01fdd78a401e81cbe'  
        
        # Run the Fargate task using the ECS RunTask API
        response = ecs_client.run_task(
            cluster=cluster_name,
            launchType='FARGATE',
            taskDefinition=task_definition,
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [subnet_id],
                    'securityGroups': [security_group_id],
                    'assignPublicIp': 'ENABLED'
                }
            },
            overrides={
                'containerOverrides': [
                    {
                        'name': 'my-task-container',
                    }
                ]
            }
        )
        
        if not response['tasks']:
            print("❌ Failed to launch task:", response.get("failures", []))
            return {
                'statusCode': 500,
                'body': f"Failed to launch task: {response.get('failures', [])}"
            }

        # Log the task details
        task_arn = response['tasks'][0]['taskArn']
        print(f"Started Fargate task with ARN: {task_arn}")

        return {
            'statusCode': 200,
            'body': f'Successfully started Fargate task with ARN: {task_arn}'
        }
        
    except Exception as e:
        print(f"Failed to start Fargate task: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }