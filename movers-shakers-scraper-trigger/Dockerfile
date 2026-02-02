FROM public.ecr.aws/lambda/python:3.10
# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

#OVERWRITE THE FIRST VALUE WITH YOUR PRIMARYFILE.py. KEEP THE "./"
COPY test.py ${LAMBDA_TASK_ROOT}/

# Specify the Lambda handler
CMD ["test.handler"]