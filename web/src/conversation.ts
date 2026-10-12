import type {Run, Workflow} from './types';
import type {AgentActivity} from './agentActivity';

export type QuestionTurn = {run: Run; previousAttempts: Run[]};

const unavailable = "The analyst's AI service isn't available right now. Please try again later, and contact your administrator if this continues.";
const rephrase = "I couldn't turn this question into a reliable answer. Try rephrasing it with the measure and period you want.";
const failures = new Map<string, string>([
  ['run_timeout', 'This question took longer than the time allowed, so I stopped. Try a narrower question, such as one measure for a specific period.'],
  ['metric_query_timeout', 'Reading the data took longer than the time allowed. Try a shorter period or fewer breakdowns.'],
  ['model_timeout', 'The AI model took too long to respond. This is usually temporary, so please retry.'],
  ['model_unavailable', 'The AI model is temporarily unavailable. Please retry in a moment.'],
  ['metric_capacity_reached', 'The analyst is busy with other questions. Please wait a moment and retry.'],
  ['model_input_budget', 'This question is too long for the analyst to work with. Shorten it or split it into separate questions.'],
  ['model_output_budget', 'This question needs a longer answer than the analyst can produce. Ask for fewer measures or a shorter period.'],
  ['run_budget_exceeded', 'This question needed more steps than one answer allows. Try splitting it into smaller questions.'],
  ['model_invalid_output', rephrase],
  ['model_invalid_plan', rephrase],
  ['clarification_limit', "I still couldn't tell what you meant after several clarifications. Ask again, stating the measure, period and any filters."],
  ['invalid_evidence', "The result didn't pass its consistency checks, so I haven't shown it. Please retry."],
  ['permissions_changed', 'Your access changed while this question was running. Retry to answer it with your current access.'],
  ['access_denied', "Your account doesn't have access to the data this question needs."],
  ['metric_not_certified', "The measure this question needs isn't available for reporting yet."],
  ['result_expired', 'The earlier answer this follow-up builds on has expired. Ask the full question again.'],
  ['result_evidence_changed', 'The data behind the earlier answer has changed. Ask the full question again for current figures.'],
  ['workflow_version_changed', 'The analyst has been updated since the earlier answer. Ask the full question again.'],
  ['exploration_disabled', "Open-ended analysis is switched off, so this follow-up can't run. Ask a question about the standard reports."],
  ['unsupported_question', 'This question needs analysis beyond the reports currently enabled.'],
  ['model_not_configured', unavailable],
  ['model_authentication_failed', unavailable],
  ['model_not_found', unavailable],
  ['model_request_rejected', unavailable],
]);

// Plain text for runs that ended without an answer; internal codes are never shown.
export function runOutcome(state: Workflow): AgentActivity {
  if (state.status === 'cancelled') return {phase: 'waiting', message: 'You cancelled this question.'};
  if (state.status === 'interrupted') return {phase: 'waiting', message: 'The analyst stopped before finishing, usually because the service restarted. Retry to ask again.'};
  return {phase: 'failed', message: failures.get(state.error_code ?? '') ?? 'Something went wrong while answering this question. Please retry.'};
}

// Fold only consecutive unsuccessful attempts of the same question. Completed
// answers and different questions keep their place in the conversation.
export function questionTurns(runs: Run[], workflows: Record<string, Workflow>): QuestionTurn[] {
  const turns: QuestionTurn[] = [];
  for (const run of [...runs].reverse()) {
    const previous = turns.at(-1);
    const state = previous && workflows[previous.run.id];
    if (previous && previous.run.question.trim() === run.question.trim()
        && !state?.answer && ['failed', 'cancelled', 'interrupted'].includes(state?.status || previous.run.status)) {
      previous.previousAttempts.push(previous.run);
      previous.run = run;
    } else turns.push({run, previousAttempts: []});
  }
  return turns;
}

export function historyTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '' : new Intl.DateTimeFormat('en-GB', {
    day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', timeZone: 'Europe/London',
  }).format(date);
}
