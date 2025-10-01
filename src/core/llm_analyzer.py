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

from openai import OpenAI
from pydantic import ValidationError
import pandas as pd

try:
    from .models import (
        ProjectFeatures,
        NumericPredictions,
        ProjectAnalysisInput,
        ProjectAnalysisOutput,
        AnalysisResult,
        SegmentStatistics
    )
    from src.config import ANALYSIS_DIR, CACHE_DIR
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

# Set up logging
logger = logging.getLogger(__name__)


class LLMAnalyzer:
    """
    LLM analyzer that provides reasoning for pre-computed numeric baselines.
    Uses OpenAI model for analysis and reasoning only.
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4o"):
        """
        Initialize the LLM analyzer.
        
        Args:
            api_key: OpenAI API key. If None, will try to get from environment
            model: OpenAI model to use (default: gpt-4o)
        """
        # Initialize OpenAI client
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key not provided. Set OPENAI_API_KEY environment variable.")
        
        self.client = OpenAI(api_key=self.api_key)
        self.model = model
        
        # Track API usage
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
        
        # Calculate confidence level
        confidence = "High" if segment_info['sample_size'] > 50 else \
                    "Medium" if segment_info['sample_size'] > 15 else "Low"
        
        prompt = f"""
You are analyzing a project with pre-computed baseline predictions. Your role is to:
1. Provide clear reasoning for why these predictions make sense
2. Identify any special factors that might adjust the rating by ±1
3. Explain the predictions in business terms

Project Data:
{json.dumps(project_json, indent=2)}

Historical Context:
- Segment used: {', '.join(segment_info['keys']) if segment_info['keys'] else 'Global data'}
- Similar projects analyzed: {segment_info['sample_size']}
- Backoff tier: {segment_info['backoff_tier']} (0=most specific, 5=global)
- Confidence level: {confidence}

Baseline Predictions (already calculated):
- Expected Gestation Period: {numeric_predictions.expected_gestation_days} days (range: {numeric_predictions.gestation_range.get('p25', 'N/A')}-{numeric_predictions.gestation_range.get('p75', 'N/A')} days)
- Expected Conversion Rate: {numeric_predictions.expected_conversion_rate:.1%} (confidence: {numeric_predictions.conversion_confidence:.1%})
- Rating Score: {numeric_predictions.rating_score}/100

Historical Performance in this segment:"""

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
        
        prompt += f"""
- Win rate: {win_rate}
- Total projects: {total_projects} (Wins: {wins}, Losses: {losses}, Open enquiries: {open_count})
- Average project value: {avg_value}
- Open enquiries simply indicate no recorded outcome yet; they must not be treated as a negative signal.

Please provide:
1. **Gestation Period Reasoning**: Why {numeric_predictions.expected_gestation_days} days makes sense for this project
2. **Conversion Rate Reasoning**: What factors support the {numeric_predictions.expected_conversion_rate:.1%} conversion rate
3. **Rating Score Reasoning**: Why this project deserves a {numeric_predictions.rating_score}/100 rating
4. **Special Factors**: Any unique aspects that might adjust the rating by ±5?

Consider these factors in your analysis:
- The account's historical performance
- The project type and category combination
- The value band relative to similar projects
- The product type's typical success rate

CRITICAL RULES - DO NOT VIOLATE:
- Open enquiries are NEUTRAL DATA POINTS. Do not mention them as risks, uncertainties, limitations, or caveats.
- Do not reference open enquiries in confidence_notes, special_factors, or any reasoning sections.
- Do not use pipeline-stage terminology (e.g., "Open Enquiry", "Won", "Lost") in your reasoning.
- Focus ONLY on: conversion rates, gestation periods, project values, and segment sample sizes.
- If discussing confidence or data quality, reference only: sample size, backoff tier, or statistical confidence metrics.

Return as JSON with structure:
{{
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
- Only suggest rating_adjustment of -5, 0, or +5
- Base your reasoning on the data provided
- Be specific about which factors most influence each prediction
- Consider the backoff tier - higher tiers mean less specific data was available
- DO NOT mention open enquiries anywhere in your response
"""
        
        return prompt
    
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
            if rating_adj not in [-5, 0, 5]:
                logger.warning(f"Invalid rating adjustment {rating_adj}, setting to 0")
                response_data["adjustments"]["rating_adjustment"] = 0
            
            # Create and validate output
            return ProjectAnalysisOutput(**response_data)
            
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(f"Failed to parse LLM response: {e}")
            logger.debug(f"Raw response: {response_text}")
            
            # Return default output on parse failure
            return ProjectAnalysisOutput(
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
        
        Args:
            project_features: Project characteristics
            numeric_predictions: Pre-computed numeric baselines
            segment_statistics: Statistics of the segment used
            historical_context: Additional historical data
            
        Returns:
            Tuple of (analysis output, metadata)
        """
        start_time = time.time()
        
        # Create prompt
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
            # Use regular create with JSON object format (skip structured parsing)
            token_param_options = [
                {"max_tokens": 1500, "temperature": 0.0},
            ]
            last_exc: Optional[Exception] = None
            response = None
            messages = [
                {
                    "role": "system",
                    "content": "You are a data analyst specializing in project evaluation and risk assessment. Provide clear, evidence-based reasoning for predictions. Treat 'Open Enquiry' as neutral (neither early-stage nor high-uncertainty). Do not use pipeline-stage wording to justify predictions; focus on numeric baselines (conversion, gestation, value) and segment statistics.",
                },
                {"role": "user", "content": prompt},
            ]
            
            # Use regular create with JSON object format (skip structured parsing)
            for token_kwargs in token_param_options:
                try:
                    response = self.client.chat.completions.create(
                        model=self.model,
                        messages=messages,
                        response_format={"type": "json_object"},
                        **token_kwargs,
                    )
                    break
                except Exception as e:
                    last_exc = e
                    continue

            if response is None:
                raise last_exc or RuntimeError("OpenAI call failed with all token parameter variants")

            # Track API usage
            self.api_calls += 1
            if response.usage:
                self.total_tokens += response.usage.total_tokens

            # Use parsed object if available, otherwise fall back to manual parse
            message = response.choices[0].message
            # If parse-helper path was used, .parsed may exist
            if getattr(message, "parsed", None):
                llm_output: ProjectAnalysisOutput = message.parsed  # already validated
            else:
                # Otherwise, parse content text manually
                content_text = getattr(message, "content", "") or ""
                llm_output = self._parse_llm_response(content_text)

            # Create metadata
            metadata = {
                "llm_model": self.model,
                "api_calls": self.api_calls,
                "response_time": time.time() - start_time,
                "tokens_used": response.usage.total_tokens if response.usage else 0,
                "finish_reason": response.choices[0].finish_reason,
            }

            logger.info(f"LLM analysis completed in {metadata['response_time']:.2f}s")

            return llm_output, metadata
            
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            
            # Return default output on API failure
            return ProjectAnalysisOutput(
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
                "special_factors": getattr(llm_output, 'special_factors', llm_output.confidence_notes)
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
        Prices as of 2025 for GPT-4o.
        """
        # GPT-4o pricing (approximate)
        input_price_per_1k = 0.00025 # $0.25 per 1M input tokens
        output_price_per_1k = 0.002 # $2 per 1M output tokens
        
        # Rough estimate (assuming 70% input, 30% output)
        estimated_input_tokens = self.total_tokens * 0.7
        estimated_output_tokens = self.total_tokens * 0.3
        
        cost = (estimated_input_tokens / 1000 * input_price_per_1k + 
                estimated_output_tokens / 1000 * output_price_per_1k)
        
        return round(cost, 4)

