export type PostDeploymentValidationsServiceConfig = {
    environment: string;
    schema: string;
    prevStartTimestamp: string;
    prevEndTimestamp: string;
    currentStartTimestamp: string;
    currentEndTimestamp: string;
}