import {useEffect, useRef, useState} from 'react';
import type {View as VegaView} from 'vega';
import type {TopLevelSpec} from 'vega-lite';
import type {ChartIntent, MetricResult} from './types';
import {chartSpec} from './presentation.mjs';

export default function Chart({result, intent, ownedId}: {result: MetricResult; intent: ChartIntent; ownedId: string}) {
  const container = useRef<HTMLDivElement>(null);
  const [failure, setFailure] = useState('');
  const {spec, reason} = chartSpec(result, intent, ownedId);
  useEffect(() => {
    let disposed = false, view: VegaView | undefined;
    setFailure('');
    if (!spec) return;
    (async () => {
      const [{compile}, {parse, View}, {expressionInterpreter}] = await Promise.all([
        import('vega-lite'), import('vega'), import('vega-interpreter')]);
      if (disposed || !container.current) return;
      const runtime = parse(compile(spec as TopLevelSpec).spec, undefined, {ast: true});
      view = new View(runtime, {renderer: 'svg', expr: expressionInterpreter,
        loader: {load: async () => {throw new Error('External chart data prohibited');},
          sanitize: async () => {throw new Error('External chart URLs prohibited');}} as never});
      await view.initialize(container.current).runAsync();
      if (disposed) view.finalize();
    })().catch(() => {if (!disposed) setFailure('Chart unavailable. Exact values are available in the table.');});
    return () => {disposed = true; view?.finalize(); container.current?.replaceChildren();};
  }, [result, intent, ownedId]);
  return <>{(reason || failure) && <p className="note">{reason || failure}</p>}<div className="chart" tabIndex={spec ? 0 : undefined} ref={container} /></>;
}
