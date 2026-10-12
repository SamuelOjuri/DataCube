export type AgentPhase = 'submitting' | 'connecting' | 'thinking' | 'planning' | 'retrieving' | 'running' | 'generating' | 'verifying' | 'waiting' | 'completed' | 'failed';
// fraction: approximate share of the run completed once this stage starts.
export type AgentActivity = {phase: AgentPhase; message: string; fraction?: number};

const stages: Record<string, AgentActivity> = {
  authorizing: {phase: 'verifying', message: 'Checking your access', fraction: .08},
  interpreting: {phase: 'thinking', message: 'Understanding your question', fraction: .14},
  retrieving_definitions: {phase: 'retrieving', message: 'Checking definitions', fraction: .2},
  planning: {phase: 'planning', message: 'Preparing the query', fraction: .32},
  exploring: {phase: 'planning', message: 'Writing a query for your question', fraction: .45},
  resolving_entities: {phase: 'verifying', message: 'Checking filters', fraction: .45},
  querying: {phase: 'running', message: 'Reading your data', fraction: .6},
  validating_evidence: {phase: 'verifying', message: 'Checking the evidence', fraction: .75},
  selecting_presentation: {phase: 'generating', message: 'Preparing your answer', fraction: .85},
  grounding_answer: {phase: 'verifying', message: 'Linking the answer to its evidence', fraction: .94},
  explaining: {phase: 'generating', message: 'Explaining what the data can show', fraction: .85},
};

export const activityForStage = (stage: unknown): AgentActivity =>
  stages[String(stage)] ?? {phase: 'thinking', message: 'Analysing your question'};
