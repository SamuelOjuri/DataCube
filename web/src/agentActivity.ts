export type AgentPhase = 'submitting' | 'connecting' | 'thinking' | 'planning' | 'retrieving' | 'running' | 'generating' | 'verifying' | 'waiting' | 'completed' | 'failed';
export type AgentActivity = {phase: AgentPhase; message: string};

const stages: Record<string, AgentActivity> = {
  authorizing: {phase: 'verifying', message: 'Checking your access'},
  interpreting: {phase: 'thinking', message: 'Understanding your question'},
  retrieving_definitions: {phase: 'retrieving', message: 'Checking definitions'},
  planning: {phase: 'planning', message: 'Preparing the query'},
  exploring: {phase: 'planning', message: 'Writing a query for your question'},
  resolving_entities: {phase: 'verifying', message: 'Checking filters'},
  querying: {phase: 'running', message: 'Reading your data'},
  validating_evidence: {phase: 'verifying', message: 'Checking the evidence'},
  selecting_presentation: {phase: 'generating', message: 'Preparing your answer'},
  grounding_answer: {phase: 'verifying', message: 'Linking the answer to its evidence'},
  explaining: {phase: 'generating', message: 'Explaining what the data can show'},
};

export const activityForStage = (stage: unknown): AgentActivity =>
  stages[String(stage)] ?? {phase: 'thinking', message: 'Analysing your question'};
