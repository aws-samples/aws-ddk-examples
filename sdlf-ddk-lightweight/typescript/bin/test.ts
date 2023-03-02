import * as cdk from 'aws-cdk-lib';
import { AppFlowIngestionStage } from "aws-ddk-core";

const app = new cdk.App();
const stack = new cdk.Stack()
const appflowStage = new AppFlowIngestionStage(stack, "appflow-ingestion", {
  flowName: "dummy-appflow-flow",
});