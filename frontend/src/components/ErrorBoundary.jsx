/**
 * ErrorBoundary.jsx — Error boundary enterprise
 * 
 * Isola i crash dei widget: se un componente crasha,
 * mostra un errore localizzato invece di abbattere tutta la pagina.
 */
import { Component } from 'react'
import { AlertCircle, RefreshCw } from 'lucide-react'

export class ErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error }
  }

  componentDidCatch(error, info) {
    // Log strutturato — in produzione andrebbe a un servizio observability
    console.error('[ErrorBoundary]', error, info.componentStack)
  }

  render() {
    if (!this.state.hasError) return this.props.children

    const { fallback, title = 'Errore nel componente' } = this.props
    if (fallback) return fallback

    return (
      <div className="flex items-start gap-3 bg-red-50 border border-red-100 rounded-xl px-4 py-3 text-sm text-red-700">
        <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" />
        <div className="flex-1">
          <div className="font-semibold">{title}</div>
          <div className="text-xs mt-0.5 text-red-600">
            {this.state.error?.message?.slice(0, 120) || 'Errore imprevisto'}
          </div>
        </div>
        <button
          onClick={() => this.setState({ hasError: false, error: null })}
          className="btn-ghost p-1.5 text-red-500 hover:text-red-700 flex-shrink-0"
          title="Riprova"
        >
          <RefreshCw className="h-3.5 w-3.5" />
        </button>
      </div>
    )
  }
}

/**
 * HOC per wrappare qualsiasi componente con error boundary.
 */
export function withErrorBoundary(Component, title) {
  return function WrappedWithBoundary(props) {
    return (
      <ErrorBoundary title={title}>
        <Component {...props} />
      </ErrorBoundary>
    )
  }
}
