import type {Run, Workflow} from './types';

export type QuestionTurn = {run: Run; previousAttempts: Run[]};

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
