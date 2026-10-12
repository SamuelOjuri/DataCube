type SignInProps = {
  busy: boolean;
  message: string;
  onLogin: () => void;
};

function RevenuePreview() {
  return <section className="signin-preview" aria-labelledby="signin-preview-title">
    <div className="signin-preview-heading">
      <h2 id="signin-preview-title">A question. A clearer picture.</h2>
      <span className="signin-sample-label">Sample data</span>
    </div>
    <div className="signin-preview-card">
      <div className="signin-preview-question">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">
          <path d="M5 5h14v11H9l-4 4V5Z" strokeLinejoin="round" />
          <path d="M8 9h8M8 12h5" strokeLinecap="round" />
        </svg>
        <p>How has revenue changed over the last six months?</p>
      </div>
      <div className="signin-preview-answer">
        <div className="signin-preview-identity">
          <span className="signin-analyst-mark" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
              <path d="m12 3 8 4.5v9L12 21l-8-4.5v-9L12 3Z" strokeLinejoin="round" />
              <path d="m4 7.5 8 4.5 8-4.5M12 12v9M8 5.25l8 4.5" />
            </svg>
          </span>
          <span>DataCube Analyst</span>
        </div>
        <p>September revenue reached <strong>£1.28m</strong>, up <strong>12.3%</strong> from August.</p>
        <div className="signin-preview-metric">
          <div><span>September invoiced revenue</span><strong>£1.28m</strong></div>
          <span className="signin-preview-change">+12.3% vs Aug</span>
        </div>
        <svg className="signin-preview-chart" viewBox="0 0 420 190" role="img" aria-labelledby="signin-chart-title signin-chart-description">
          <title id="signin-chart-title">Sample monthly invoiced revenue</title>
          <desc id="signin-chart-description">Illustrative revenue: April £760,000; May £840,000; June £790,000; July £980,000; August £1,140,000; September £1,280,000.</desc>
          <g className="signin-chart-grid">
            <path d="M48 24H412M48 68H412M48 112H412M48 156H412" />
          </g>
          <g className="signin-chart-labels">
            <text x="0" y="28">£1.5m</text><text x="0" y="72">£1m</text><text x="0" y="116">£0.5m</text><text x="0" y="160">£0</text>
            <text x="74" y="181" textAnchor="middle">Apr</text><text x="136" y="181" textAnchor="middle">May</text>
            <text x="198" y="181" textAnchor="middle">Jun</text><text x="260" y="181" textAnchor="middle">Jul</text>
            <text x="322" y="181" textAnchor="middle">Aug</text><text x="384" y="181" textAnchor="middle">Sep</text>
          </g>
          <g className="signin-chart-bars">
            <rect x="57" y="89.12" width="34" height="66.88" rx="4" />
            <rect x="119" y="82.08" width="34" height="73.92" rx="4" />
            <rect x="181" y="86.48" width="34" height="69.52" rx="4" />
            <rect x="243" y="69.76" width="34" height="86.24" rx="4" />
            <rect x="305" y="55.68" width="34" height="100.32" rx="4" />
          </g>
          <rect className="signin-chart-latest" x="367" y="43.36" width="34" height="112.64" rx="4" />
        </svg>
        <div className="signin-preview-source">
          <span>Source: Invoices</span><span>Scope: Apr–Sep</span>
        </div>
      </div>
    </div>
    <p className="signin-preview-caption">Illustrative preview. Your company data appears after sign-in.</p>
  </section>;
}

export default function SignIn({busy, message, onLogin}: SignInProps) {
  return <main id="workspace" className="signin">
    <div className="signin-layout">
      <section className="signin-intro" aria-labelledby="signin-title">
        <p className="eyebrow">YOUR COMPANY. CLEARER.</p>
        <h1 id="signin-title">Your data,<br />in conversation.</h1>
        <p className="signin-description">Ask questions about enquiries, orders and revenue. Get clear answers backed by your company data.</p>
        <div className="signin-access">
          <button className="signin-button" disabled={busy} onClick={onLogin} aria-describedby="signin-access-note">
            <svg className="signin-monday-mark" viewBox="0 0 32 24" fill="currentColor" aria-hidden="true">
              <rect x="4" y="2" width="7" height="21" rx="3.5" transform="rotate(30 7.5 12.5)" />
              <rect x="16" y="2" width="7" height="21" rx="3.5" transform="rotate(30 19.5 12.5)" />
              <circle cx="29" cy="18.5" r="3" />
            </svg>
            <span>{busy ? 'Please wait…' : 'Continue with Monday'}</span>
            <svg className="signin-button-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">
              <path d="M5 12h14m-5-5 5 5-5 5" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </button>
          <p id="signin-access-note" className="signin-access-note">
            <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.3" aria-hidden="true">
              <rect x="4.5" y="8.5" width="11" height="8" rx="2" /><path d="M7 8.5V6a3 3 0 0 1 6 0v2.5M10 12v1.5" strokeLinecap="round" />
            </svg>
            Access is available to approved team members.
          </p>
          <p className="signin-status" role="status">{message}</p>
        </div>
        <p className="signin-assurance">
          <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.3" aria-hidden="true">
            <path d="M5 2.5h7l3 3v12H5v-15Zm7 0v3h3M7.5 9h5M7.5 12h5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          Every answer comes with its source and scope.
        </p>
      </section>
      <RevenuePreview />
    </div>
    <footer className="signin-footer"><span>Company intelligence, in conversation.</span><span>Built for TaperedPlus</span></footer>
  </main>;
}
