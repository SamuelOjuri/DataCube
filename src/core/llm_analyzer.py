"""
Stage 4: LLM Integration for Reasoning
Provides reasoning for pre-computed numeric baselines using OpenAI GPT-4o
"""

import json
import logging
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
import time
from pathlib import Path
import os
from textwrap import dedent

from google import genai
from google.genai import types as genai_types
from google.api_core import exceptions as google_exceptions
from pydantic import ValidationError
import pandas as pd

# logger = logging.getLogger(__name__)
# _env_level = os.getenv("LLM_ANALYZER_LOG_LEVEL", "").upper()
# if _env_level:
#     logger.setLevel(getattr(logging, _env_level, logging.INFO))

try:
    from .models import (
        ProjectFeatures,
        NumericPredictions,
        ProjectAnalysisInput,
        ProjectAnalysisOutput,
        AnalysisResult,
        SegmentStatistics
    )
    from src.config import ANALYSIS_DIR, CACHE_DIR, GEMINI_API_KEY, GEMINI_MODEL
except ImportError:
    from models import (
        ProjectFeatures,
        NumericPredictions,
        ProjectAnalysisInput,
        ProjectAnalysisOutput,
        AnalysisResult,
        SegmentStatistics
    )
    from config import ANALYSIS_DIR, CACHE_DIR

LLM_RESPONSE_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    properties={
        "summary": genai_types.Schema(
            type=genai_types.Type.STRING,
            description="Plain-language overview for non-technical stakeholders."
        ),
        "reasoning": genai_types.Schema(
            type=genai_types.Type.OBJECT,
            properties={
                "gestation": genai_types.Schema(type=genai_types.Type.STRING),
                "conversion": genai_types.Schema(type=genai_types.Type.STRING),
                "rating": genai_types.Schema(type=genai_types.Type.STRING),
            },
            required=["gestation", "conversion", "rating"],
        ),
        "adjustments": genai_types.Schema(
            type=genai_types.Type.OBJECT,
            properties={
                "rating_adjustment": genai_types.Schema(
                    type=genai_types.Type.INTEGER,
                    description="Whole-number rating adjustment between -5 and +5."
                )
            },
            required=["rating_adjustment"],
        ),
        "special_factors": genai_types.Schema(type=genai_types.Type.STRING),
        "confidence_notes": genai_types.Schema(type=genai_types.Type.STRING),
    },
    required=["summary", "reasoning", "adjustments", "confidence_notes"],
)

# Set up logging
logger = logging.getLogger(__name__)


class LLMAnalyzer:
    """
    LLM analyzer that provides reasoning for pre-computed numeric baselines.
    Uses Google Gemini model for analysis and reasoning only.
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        *,
        max_token_retries: int = 2,
    ):
        """
        Initialize the LLM analyzer.

        Args:
            api_key: Gemini API key. If None, pulled from environment.
            model: Gemini model to use (defaults to config GEMINI_MODEL).
            max_token_retries: Number of retries when hitting token limit.
        """
        self.api_key = api_key or GEMINI_API_KEY
        if not self.api_key:
            raise ValueError(
                "Gemini API key not provided. Set GEMINI_API_KEY or GOOGLE_API_KEY."
            )

        self.client = genai.Client(api_key=self.api_key)
        self.model = model or GEMINI_MODEL
        self.max_token_retries = max(0, max_token_retries)

        self.api_calls = 0
        self.total_tokens = 0

        logger.info(f"LLM Analyzer initialized with model: {self.model}")
    
    def _create_analysis_prompt(
        self,
        project_features: ProjectFeatures,
        numeric_predictions: NumericPredictions,
        segment_statistics: SegmentStatistics,
        historical_context: Dict[str, Any]
    ) -> str:
        """
        Create a structured prompt for LLM analysis.
        
        Args:
            project_features: Features of the project
            numeric_predictions: Pre-computed numeric baselines
            segment_statistics: Statistics of the segment used
            historical_context: Historical data context
            
        Returns:
            Formatted prompt string
        """
        # Format project features
        project_json = {
            "project_id": project_features.project_id,
            "account": project_features.account or "Unknown",
            "type": project_features.type or "Unknown",
            "category": project_features.category or "Unknown",
            "product_type": project_features.product_type or "Unknown",
            "new_enquiry_value": f"£{project_features.new_enquiry_value:,.2f}",
            "value_band": project_features.value_band or "Unknown",
            "pipeline_stage": project_features.pipeline_stage or "Unknown"
        }
        
        # Format segment information
        segment_info = {
            "keys": segment_statistics.segment_keys if segment_statistics else [],
            "sample_size": segment_statistics.sample_size if segment_statistics else 0,
            "backoff_tier": segment_statistics.backoff_tier if segment_statistics else 5
        }
        
        sample_size = segment_info["sample_size"]
        backoff_tier = segment_info["backoff_tier"]
        conv_conf = numeric_predictions.conversion_confidence or 0.0
        gest_conf = numeric_predictions.gestation_confidence or 0.0
        conv_conf_pct = f"{conv_conf:.0%}"
        gest_conf_pct = f"{gest_conf:.0%}"
        
        # Calculate confidence level
        confidence = "High" if segment_info['sample_size'] > 50 else \
                    "Medium" if segment_info['sample_size'] > 15 else "Low"
        
        # Format historical performance separately to avoid f-string issues
        if segment_statistics and segment_statistics.conversion_rate:
            win_rate = f"{segment_statistics.conversion_rate:.1%}"
        else:
            win_rate = "N/A"
        
        if segment_statistics:
            total_projects = segment_statistics.wins + segment_statistics.losses + segment_statistics.open
            wins = segment_statistics.wins
            losses = segment_statistics.losses
            open_count = segment_statistics.open
        else:
            total_projects = wins = losses = open_count = "N/A"
        
        if segment_statistics and segment_statistics.average_value:
            avg_value = f"£{segment_statistics.average_value:,.2f}"
        else:
            avg_value = "N/A"
        
        prompt = f"""
        You are analyzing a project with pre-computed baseline predictions. Your role is to:
        1. Provide clear reasoning for why these predictions make sense
        2. Identify any special factors that might adjust the rating by ±1
        3. Explain the predictions in business terms

        Speak directly to a business stakeholder with no analytics background. Use short sentences, avoid jargon, and translate percentages into relatable odds (e.g., "around one in five"). When you mention confidence, say what it means in practice (e.g., "we're fairly sure, but there is some room for surprises"). Example tone: "We usually see a decision in about seven weeks because around 30 similar refurb jobs wrapped up in that window last year."

        Project Data:
        {json.dumps(project_json, indent=2)}

        Historical Context:
        - Segment used: {', '.join(segment_info['keys']) if segment_info['keys'] else 'Global data'}
        - Similar projects analyzed: {segment_info['sample_size']}
        - Backoff tier: {segment_info['backoff_tier']} (0=most specific, 5=global)
        - Confidence level: {confidence}
        - Conversion confidence: {conv_conf_pct}
        - Gestation confidence: {gest_conf_pct}

        Baseline Predictions (already calculated):
        - Expected Gestation Period: {numeric_predictions.expected_gestation_days} days (range: {numeric_predictions.gestation_range.get('p25', 'N/A')}-{numeric_predictions.gestation_range.get('p75', 'N/A')} days)
        - Expected Conversion Rate: {numeric_predictions.expected_conversion_rate:.1%} (confidence: {numeric_predictions.conversion_confidence:.1%})
        - Rating Score: {numeric_predictions.rating_score}/100

        Historical Performance in this segment:
        - Win rate: {win_rate}
        - Total projects: {total_projects} (Wins: {wins}, Losses: {losses}, Open enquiries: {open_count})
        - Average project value: {avg_value}
        - Open enquiries simply indicate no recorded outcome yet; they must not be treated as a negative signal.

        Adjustment Guardrails (non-negotiable):
        - rating_adjustment must be an integer between -5 and +5. Zero is the default and should be used unless a statistically significant deviation is proven.
        - Only consider a non-zero adjustment if every one of these conditions is satisfied:
          * sample_size ≥ 60 (current: {sample_size})
          * conversion confidence ≥ 70% (current: {conv_conf_pct})
          * backoff tier ≤ 2 (current: {backoff_tier})
          * You identify and cite a quantified deviation from baselines (e.g., segment win rate vs baseline conversion, project value vs segment average) large enough to justify the magnitude you choose.
        - If any condition is not met, you must leave rating_adjustment at 0 and explicitly state that no statistically significant adjustment is justified.
        - When you do apply a non-zero adjustment, you must reference the exact metrics (sample size, conversion confidence, quantified deviation) inside both reasoning.rating and special_factors.

        Please provide:
        0. **Plain-English Overview**: Two or three sentences or bullet points that summarise the key takeaways in everyday language.
        1. **Gestation Period Reasoning**: Why {numeric_predictions.expected_gestation_days} days makes sense for this project
        2. **Conversion Rate Reasoning**: What factors support the {numeric_predictions.expected_conversion_rate:.1%} conversion rate
        3. **Rating Score Reasoning**: Why this project deserves a {numeric_predictions.rating_score}/100 rating
        4. **Special Factors**: Any unique aspects that might adjust the rating by ±5?

        Consider these factors in your analysis:
        - The account's historical performance
        - The project type and category combination
        - The value band relative to similar projects
        - The product type's typical success rate

        Rules:
        - Treat 'Open Enquiry' as neutral; do not infer stage or uncertainty, and do not describe a high count of open enquiries as a risk.
        - Do not use pipeline-stage wording as justification; focus on numeric baselines (conversion, gestation, value) and segment statistics.
        - Cite the concrete metrics (sample size, confidence, win/value comparisons) that underpin every conclusion.
        - Every time you mention a percentage or confidence, add a short interpretation in plain English.
        - Avoid analytics jargon ("quantile", "standard deviation"); use everyday alternatives ("middle of the range", "spread").

        Return as JSON with structure:
        {{
        "summary": "An overview of the analysis...",
        "reasoning": {{
            "gestation": "Clear explanation of why the gestation period makes sense...",
            "conversion": "Explanation of conversion rate factors...",
            "rating": "Justification for the rating score..."
        }},
        "adjustments": {{
            "rating_adjustment": 0
        }},
        "special_factors": "Any notable considerations or risk factors...",
        "confidence_notes": "Any caveats about data quality or sample size..."
        }}

        IMPORTANT: 
        - rating_adjustment must stay between -5 and +5 (inclusive) and be a whole number
        - Base your reasoning on the data provided and the guardrails above
        - Be specific about which factors most influence each prediction
        - Consider the backoff tier - higher tiers mean less specific data was available
        """
        
        return prompt

    def _extract_response_content(self, response: Any) -> str:
        """Normalize structured Gemini responses into a JSON string."""
        text = getattr(response, "text", None)
        if text:
            return text

        chunks: List[str] = []
        candidates = getattr(response, "candidates", None) or []
        for candidate in candidates:
            content = getattr(candidate, "content", None)
            parts = getattr(content, "parts", None) if content else None
            if not parts:
                continue
            for part in parts:
                chunk = self._normalize_part(part)
                if chunk:
                    chunks.append(chunk)

        return "".join(chunks)

    def _normalize_part(self, part: Any) -> Optional[str]:
        part_text = getattr(part, "text", None)
        if part_text:
            return part_text

        def _as_json(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, str):
                return value
            try:
                if hasattr(value, "model_dump_json"):
                    return value.model_dump_json()
                if hasattr(value, "model_dump"):
                    return json.dumps(value.model_dump())
                if hasattr(value, "to_dict"):
                    return json.dumps(value.to_dict())
                if hasattr(value, "items"):
                    return json.dumps(dict(value))
                if isinstance(value, (dict, list, tuple)):
                    return json.dumps(value)
                # Fallback: try interpreting stringified form as JSON
                return json.dumps(json.loads(str(value)))
            except (TypeError, ValueError, json.JSONDecodeError):
                logger.debug("Unable to normalise part value %r", value)
                return None

        func_call = getattr(part, "function_call", None)
        if func_call:
            normalized = _as_json(getattr(func_call, "args", None))
            if normalized:
                return normalized

        json_value = getattr(part, "json_value", None)
        normalized = _as_json(json_value)
        if normalized:
            return normalized

        return None
    
    def _parse_llm_response(self, response_text: str) -> ProjectAnalysisOutput:
        """
        Parse and validate LLM response.
        
        Args:
            response_text: Raw response from LLM
            
        Returns:
            Validated ProjectAnalysisOutput
        """
        try:
            # Quick guard: empty or whitespace-only
            if not response_text or not response_text.strip():
                raise json.JSONDecodeError("Empty response content", response_text or "", 0)
            # Try to extract JSON from response
            # Handle cases where LLM might include markdown formatting
            if "```json" in response_text:
                json_start = response_text.find("```json") + 7
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end]
            elif "```" in response_text:
                json_start = response_text.find("```") + 3
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end]
            
            # Parse JSON
            response_data = json.loads(response_text.strip())

            # Ensure required fields exist
            if "summary" not in response_data or not response_data.get("summary"):
                response_data["summary"] = "Plain-English summary unavailable."
            if "reasoning" not in response_data:
                response_data["reasoning"] = {
                    "gestation": "Unable to provide reasoning",
                    "conversion": "Unable to provide reasoning",
                    "rating": "Unable to provide reasoning"
                }
            
            if "adjustments" not in response_data:
                response_data["adjustments"] = {"rating_adjustment": 0}
            
            if "confidence_notes" not in response_data:
                response_data["confidence_notes"] = response_data.get(
                    "special_factors", 
                    "Analysis based on available data"
                )
            
            # Validate rating adjustment is within bounds
            rating_adj = response_data["adjustments"].get("rating_adjustment", 0)
            if not isinstance(rating_adj, int) or rating_adj < -5 or rating_adj > 5:
                logger.warning(f"Invalid rating adjustment {rating_adj}, setting to 0")
                response_data["adjustments"]["rating_adjustment"] = 0
            
            # Create and validate output
            return ProjectAnalysisOutput(**response_data)
            
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(f"Failed to parse LLM response: {e}")
            logger.debug(f"Raw response: {response_text}")
            
            # Return default output on parse failure
            return ProjectAnalysisOutput(
                summary="Plain-English summary unavailable.",
                reasoning={
                    "gestation": "Error parsing LLM response",
                    "conversion": "Error parsing LLM response",
                    "rating": "Error parsing LLM response"
                },
                adjustments={"rating_adjustment": 0},
                confidence_notes="Failed to parse LLM response, using baseline predictions only"
            )

    def _build_confidence_notes(self, stats: Optional[SegmentStatistics], preds: NumericPredictions) -> str:
        n = stats.sample_size if stats else 0
        tier = stats.backoff_tier if stats else 5
        conv_conf = preds.conversion_confidence or 0.0
        gest_conf = preds.gestation_confidence or 0.0
        return (
            f"Confidence reflects sample size {n} (backoff tier {tier}), "
            f"conversion confidence {conv_conf:.0%} and gestation confidence {gest_conf:.0%}. "
            f"Pipeline stage labels (e.g., 'Open Enquiry') are treated as neutral and are not used."
        )
    
    def analyze_project(
        self,
        project_features: ProjectFeatures,
        numeric_predictions: NumericPredictions,
        segment_statistics: Optional[SegmentStatistics] = None,
        historical_context: Optional[Dict[str, Any]] = None
    ) -> Tuple[ProjectAnalysisOutput, Dict[str, Any]]:
        """
        Analyze a project using LLM for reasoning.
        """
        start_time = time.time()

        prompt = self._create_analysis_prompt(
            project_features,
            numeric_predictions,
            segment_statistics or SegmentStatistics(
                segment_keys=[],
                sample_size=0,
                backoff_tier=5
            ),
            historical_context or {}
        )

        try:
            prompt_text = dedent(f"""
                You are a data analyst specializing in project evaluation and risk assessment.
                Provide concise, evidence-based reasoning for each prediction, grounded in quantitative metrics such as conversion rates, gestation periods, and project values.
                Explain every point in plain English so that non-technical stakeholders can understand it easily.
                Start with a short, friendly summary before the detailed sections.
                Translate percentages into everyday odds where helpful (e.g., "roughly one in five").
                Keep each reasoning section to 2-3 short sentences and avoid jargon.
                Treat 'Open Enquiry' as a neutral category (neither early-stage nor high-uncertainty).
                Do NOT justify predictions using qualitative or pipeline-stage terminology.
                Focus strictly on numeric baselines, statistical trends, and segment-level insights when forming conclusions.

                {prompt}
            """)

            response = None
            finish_reason = None

            for attempt in range(self.max_token_retries + 1):
                try:
                    response = self.client.models.generate_content(
                        model=self.model,
                        contents=[{"role": "user", "parts": [{"text": prompt_text}]}],
                        config=genai_types.GenerateContentConfig(
                            temperature=0.0,
                            response_mime_type="application/json",
                            response_schema=LLM_RESPONSE_SCHEMA,
                        ),
                    )
                except google_exceptions.ServiceUnavailable as svc_err:
                    if attempt == self.max_token_retries:
                        raise
                    sleep_for = 2 ** attempt
                    logger.warning(
                        "Gemini 503 (attempt %s/%s); backing off %ss",
                        attempt + 1,
                        self.max_token_retries + 1,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                    continue

                finish_reason = getattr(
                    response.candidates[0], "finish_reason", None
                ) if getattr(response, "candidates", None) else None
                if finish_reason != genai_types.FinishReason.MAX_TOKENS:
                    break
                logger.warning(
                    "LLM output truncated by service limit (attempt %s/%s); retrying",
                    attempt + 1,
                    self.max_token_retries + 1,
                )
            else:
                logger.error("LLM response still truncated after retries")

            self.api_calls += 1
            usage = getattr(response, "usage_metadata", None)
            total_tokens_used = getattr(usage, "total_token_count", 0) if usage else 0
            self.total_tokens += total_tokens_used

            content_text = self._extract_response_content(response)
            llm_output = self._parse_llm_response(content_text)

            metadata = {
                "llm_model": self.model,
                "api_calls": self.api_calls,
                "response_time": time.time() - start_time,
                "tokens_used": total_tokens_used,
                "finish_reason": finish_reason,
            }

            logger.info(f"LLM analysis completed in {metadata['response_time']:.2f}s")

            return llm_output, metadata

        except google_exceptions.ServiceUnavailable as svc_err:
            logger.error("LLM analysis failed due to service overload: %s", svc_err)
            return ProjectAnalysisOutput(
                summary="Plain-English summary unavailable.",
                reasoning={
                    "gestation": "LLM temporarily unavailable",
                    "conversion": "LLM temporarily unavailable",
                    "rating": "LLM temporarily unavailable"
                },
                adjustments={"rating_adjustment": 0},
                confidence_notes="LLM service overloaded; try again later."
            ), {
                "error": f"Service unavailable: {svc_err}",
                "response_time": time.time() - start_time,
            }
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")

            return ProjectAnalysisOutput(
                summary="Plain-English summary unavailable.",
                reasoning={
                    "gestation": "LLM analysis unavailable",
                    "conversion": "LLM analysis unavailable",
                    "rating": "LLM analysis unavailable"
                },
                adjustments={"rating_adjustment": 0},
                confidence_notes=f"LLM analysis failed: {str(e)}"
            ), {
                "error": str(e),
                "response_time": time.time() - start_time
            }
    
    def create_final_analysis(
        self,
        project_features: ProjectFeatures,
        numeric_predictions: NumericPredictions,
        llm_output: ProjectAnalysisOutput,
        metadata: Dict[str, Any]
    ) -> AnalysisResult:
        """
        Create final analysis combining numeric and LLM outputs.
        
        Args:
            project_features: Project characteristics
            numeric_predictions: Numeric baseline predictions
            llm_output: LLM analysis output
            metadata: Analysis metadata
            
        Returns:
            Complete AnalysisResult
        """
        # Apply LLM adjustments
        adjusted_rating = numeric_predictions.rating_score + llm_output.adjustments.get("rating_adjustment", 0)
        adjusted_rating = max(1, min(100, adjusted_rating))  # Ensure within bounds
        
        # Update predictions with adjustments
        final_predictions = numeric_predictions.copy()
        if adjusted_rating != numeric_predictions.rating_score:
            final_predictions.rating_score = adjusted_rating
            logger.info(f"Rating adjusted from {numeric_predictions.rating_score} to {adjusted_rating}")
        
        # Create final result
        final_confidence_notes = self._build_confidence_notes(
            numeric_predictions.segment_statistics,
            numeric_predictions
        )
        result = AnalysisResult(
            project_id=project_features.project_id,
            analysis_timestamp=datetime.now(),
            project_features=project_features,
            predictions=final_predictions,
            reasoning=llm_output.reasoning,
            analysis_metadata={
                **metadata,
                "adjustments_applied": llm_output.adjustments,
                "confidence_notes": final_confidence_notes,
                "special_factors": getattr(llm_output, 'special_factors', llm_output.confidence_notes),
                "plain_english_summary": llm_output.summary
            }
        )
        
        return result
    
    def batch_analyze(
        self,
        projects_df: pd.DataFrame,
        numeric_predictions_dict: Dict[str, NumericPredictions],
        segment_statistics_dict: Optional[Dict[str, SegmentStatistics]] = None,
        max_projects: Optional[int] = None,
        save_results: bool = True
    ) -> List[AnalysisResult]:
        """
        Analyze multiple projects in batch.
        
        Args:
            projects_df: DataFrame with project features
            numeric_predictions_dict: Dictionary of project_id -> NumericPredictions
            segment_statistics_dict: Optional dictionary of project_id -> SegmentStatistics
            max_projects: Maximum number of projects to analyze (for testing)
            save_results: Whether to save results to file
            
        Returns:
            List of AnalysisResult objects
        """
        results = []
        projects_to_analyze = list(numeric_predictions_dict.keys())
        
        if max_projects:
            projects_to_analyze = projects_to_analyze[:max_projects]
        
        logger.info(f"Starting batch analysis for {len(projects_to_analyze)} projects")
        
        for i, project_id in enumerate(projects_to_analyze, 1):
            logger.info(f"Analyzing project {i}/{len(projects_to_analyze)}: {project_id}")
            
            # Get project features
            project_row = projects_df[projects_df['project_id'] == project_id].iloc[0]
            project_features = ProjectFeatures(
                project_id=project_id,
                name=project_row.get('name', ''),
                account=project_row.get('account'),
                type=project_row.get('type'),
                category=project_row.get('category'),
                product_type=project_row.get('product_type'),
                new_enquiry_value=project_row.get('new_enquiry_value', 0),
                gestation_period=project_row.get('gestation_period'),
                pipeline_stage=project_row.get('pipeline_stage'),
                status_category=project_row.get('status_category'),
                value_band=project_row.get('value_band')
            )
            
            # Get predictions and statistics
            numeric_predictions = numeric_predictions_dict[project_id]
            segment_stats = segment_statistics_dict.get(project_id) if segment_statistics_dict else None
            
            # Analyze with LLM
            llm_output, metadata = self.analyze_project(
                project_features,
                numeric_predictions,
                segment_stats
            )
            
            # Create final analysis
            analysis_result = self.create_final_analysis(
                project_features,
                numeric_predictions,
                llm_output,
                metadata
            )
            
            results.append(analysis_result)
            
            # Rate limiting
            if i < len(projects_to_analyze):
                time.sleep(0.5)  # Avoid hitting rate limits
        
        # Save results if requested
        if save_results and results:
            self._save_batch_results(results)
        
        logger.info(f"Batch analysis completed. Analyzed {len(results)} projects")
        logger.info(f"Total API calls: {self.api_calls}, Total tokens: {self.total_tokens}")
        
        return results
    
    def _save_batch_results(self, results: List[AnalysisResult]):
        """Save batch analysis results to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = ANALYSIS_DIR / f"llm_analysis_{timestamp}.json"
        
        # Convert results to dictionaries
        results_dict = []
        for result in results:
            result_dict = result.dict()
            # Convert datetime to string
            result_dict['analysis_timestamp'] = result.analysis_timestamp.isoformat()
            results_dict.append(result_dict)
        
        # Save to file
        with open(output_file, 'w') as f:
            json.dump(results_dict, f, indent=2, default=str)
        
        logger.info(f"Results saved to {output_file}")
    
    def get_usage_statistics(self) -> Dict[str, Any]:
        """Get API usage statistics."""
        return {
            "total_api_calls": self.api_calls,
            "total_tokens_used": self.total_tokens,
            "average_tokens_per_call": self.total_tokens / self.api_calls if self.api_calls > 0 else 0,
            "estimated_cost": self._estimate_cost()
        }
    
    def _estimate_cost(self) -> float:
        """
        Estimate API cost based on token usage.
        Prices aligned with Gemini 2.5 Flash (update when pricing changes).
        """
        # Approximate Gemini 2.5 Flash pricing (USD)
        input_price_per_1k = 0.00018
        output_price_per_1k = 0.00054
        
        # Rough estimate (assuming 70% input, 30% output)
        estimated_input_tokens = self.total_tokens * 0.7
        estimated_output_tokens = self.total_tokens * 0.3
        
        cost = (estimated_input_tokens / 1000 * input_price_per_1k + 
                estimated_output_tokens / 1000 * output_price_per_1k)
        
        return round(cost, 4)