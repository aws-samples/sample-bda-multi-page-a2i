Process multi-page documents with human review using Amazon Bedrock Data Automation and Amazon Augmented AI

Introduction

In today's data-driven business environment, extracting accurate information from documents efficiently is critical for operational success. Organizations across industries face challenges with high volumes of multipage documents that require intelligent processing. While automation has made tremendous strides, there remains a need for human expertise in specific scenarios to ensure data accuracy and quality. 

In March 2025, AWS launched Amazon Bedrock Data Automation (BDA) that enables developers to automate the generation of valuable insights from unstructured multimodal content, including documents, images, video, and audio. On the document side, BDA streamlines document processing workflows by automating extraction, transformation, and generation of insights from unstructured content. It eliminates time-consuming tasks like data preparation, model management, fine-tuning, prompt engineering, and orchestration through a unified, multi-modal inference API, delivering industry-leading accuracy at lower cost than alternative solutions.

BDA simplifies complex document processing tasks including document splitting, classification, extraction, normalization, and validation while incorporating visual grounding with confidence scores for explainability and built-in hallucination mitigation, ensuring trustworthy insights from unstructured data sources. While BDA's advanced capabilities deliver exceptional automation, there remain scenarios where human judgment is invaluable. This is where the integration with Amazon Augmented AI (A2I) creates a powerful end-to-end solution. By incorporating human review loops into the document processing workflow, organizations can ensure the highest levels of accuracy while maintaining processing efficiency. Human loops allow organizations to:

* Validate AI predictions when confidence is low
* Handle edge cases and exceptions effectively
* Ensure regulatory compliance through appropriate oversight
* Maintain high accuracy while maximizing automation
* Create feedback loops to improve model performance over time

By implementing human loops strategically, organizations can focus human attention only on uncertain portions of documents while allowing automated systems to handle routine extractions, creating an optimal balance between efficiency and accuracy.

Understanding Confidence Scores

Confidence scores are crucial in determining when to invoke human review. Confidence score are percentage of certainty that BDA has that your extraction is accurate. 

Our goal is to simplify the IDP process by handling the heavy lifting of accuracy calculation within BDA’s fully managed service. This allows customers to focus on solving their business challenges with BDA rather than dealing with complex scoring mechanisms. BDA optimizes its models for Expected Calibration Error (ECE), a metric that ensures better calibration, leading to more reliable and accurate confidence scores. 

In document processing workflows, confidence scores are generally interpreted as:

* High confidence (90-100%): The model is highly certain about its extraction
* Medium confidence (70-89%): Reasonable certainty with some potential for error
* Low confidence (<70%): High uncertainty, likely requiring human verification

We recommend testing BDA on your own specific datasets to determine the confidence threshold and triggering human review workflow. 

Solution Overview

The following architecture provides a serverless solution for processing multi-page documents with human review loops using Amazon Bedrock Data Automation and Amazon SageMaker Augmented AI:
Steps :

1. Document Ingestion - Documents are uploaded to an Amazon S3 input bucket which serves as entry point for all the documents processed through BDA.
2. Workflow Trigger - An Amazon EventBridge rule automatically detects new objects in the S3 bucket and triggers the AWS Step Functions workflow that orchestrates the entire document processing pipeline.
3. Bedrock Data Automation Processing - Within the Step Functions workflow, the "bda-document-processor" Lambda function is executed, which invokes Amazon Bedrock Data Automation (BDA) with the appropriate blueprint. BDA uses these pre-configured instructions to extract and process information from the document.
4. Extraction Output Storage - BDA analyzes the document, extracts key fields with associated confidence scores, and stores the processed output in another S3 bucket. This output contains all the extracted information and corresponding confidence levels.
5. Classification and Confidence Evaluation - The Step Functions workflow then executes the "bda-classifier" Lambda function, which retrieves the BDA output from S3. This Lambda evaluates the confidence scores against predefined thresholds for all extracted fields. This lambda also copies the bda output and stores it in aggregated_result folder in the same S3 bucket. 
6. Human Review Process - For fields with confidence scores below the threshold, the workflow routes the document to SageMaker A2I for human review. Using the custom UI, humans review the tasks and validate all fields from all pages. Reviewers can correct any fields that were incorrectly extracted by the automated process.
7. Human Review Output - The validated and corrected form data from human review is stored in an S3 bucket
8. bda-a2i aggregator - Once A2I output is written to S3, it executes the “bda-a2i-aggregator” lambda which updates the payload of bda output with the new value which was reviewed by human. This aggregated output is stored in S3. 

Deployment

To deploy this solution, you need AWS CDK, Node.js, and Docker installed on your deployment machine. A build script performs the packaging and deployment of the solution.



To deploy the solution

1. Clone the solution repository to your deployment machine.
2. Navigate to the project directory and run the build script: 


./build.sh


The deployment creates the following resources in your AWS account:

* Two new S3 buckets - one for the initial upload of documents and one for the output of documents
* A Bedrock Data Automation Project along with 5 Blueprints used to process the test document
* A Cognito User Pool for the Private Workforce that Ground Truth provides to SageMaker Augmented AI for data that is below a confidence score.
* Two Lambdas and a Step Function used to process the test documents
* Two ECR Container Images used for the Lambdas to process the test documents


After the build is done, you will need to add a worker to the Private Workforce in Ground Truth.  In order to do this, follow these steps:

Navigate to the SageMaker AI console and open Ground Truth → Labeling Workforces → Private.  You will be presented with a screen like the following image:

Scroll down to the Workers section, click the Invite new workers button, which is highlighted in the image below. 

You will be presented with a new screen that allows you to add workers by email address.  Fill in at least one email address, using one that you have access to, and click the Invite new workers button.  The following image shows the screen used to add new workers.


After the worker has been added, they will receive an email with a temporary password.  This process may take up to 5 minutes before the email is received.  Back on the Labeling Workforces screen, you will need to click on the Labeling portal sign-in URL link.  See the image below for where this link is located on the screen:

When clicking this link, a new tab will be opened that displays the Cognito login page.  You will need to supply the email address that you used earlier to setup a worker and the temporary password that was emailed.  Using both of these pieces of information, fill in the necessary values and click the Sign In button to start the login flow.  See the image below for the Cognito login page:

You will then be presented with a Change Password page.  Fill in the new password and the verification information.  Once complete, you will then be redirected to a job queue page for the private labeling workforce.  At the top of the page there will be a notice that you are not a member of a work team yet.  You will need to complete that process in the next step in order to ensure that jobs are properly assigned.  Below is an image of the job queue screen with the associated notice:
At this point, you will need to navigate back to Ground Truth → Labeling Workforces → Private and add the verified user to the work team.  Click on the Private team, which is called bda-workforce.  See the image below for the location of the link to click:

You will be presented with a screen that contains details on the private work team.  Navigate to the workers tab as seen in the image below:
At this point, you will be presented with a screen that contains a list of the workers for the team.  You will need to click the Add workers to team button, and add the recently verified user to the team.  Below is an image of the screen:


Testing

In order to test the solution, you can upload the test document located in the assets folder of the project to the S3 Bucket used for incoming documents.  You can monitor the progress of the system by navigating to the Step Function console, or reviewing the logs through CloudWatch.  After the document is processed, you will see a new job queued up for the user in Augmented AI.  To view this, navigate back to the Private workforce page and click the link for the Labeling portal sign-in URL as seen in the image below:

After you login using the email address and updated password from earlier, you will be presented with a page that displays the jobs to be reviewed, see the following image:

By clicking on the Start working button, you will be presented with a UI to review each item that was below a confidence score (defaulted to 70%) for the processed document.  The displayed screen will look like the following image:

On this screen, you will be able to modify any of the data to the corrected values.  The updated data will then be saved in the S3 output bucket in the a2i-output / bda-review-flow-definition / <date> / review-loop-<date time stamp> / output.json file.  This data can then be processed and used to provide the corrected values for information retrieved from the document.

Conclusion

The combination of Amazon Bedrock Data Automation and Amazon Augmented AI represents a transformative approach to document processing—one that delivers both automation efficiency and human-level accuracy both on single page and multi-page. 

