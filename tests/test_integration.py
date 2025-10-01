"""
Integration test for complete pipeline: Stages 1-4
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
import sys
import os
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.config import CACHE_DIR, MONDAY_API_KEY
from src.core.monday_client import MondayClient
from src.core.enhanced_extractor import EnhancedMondayExtractor
from src.core.data_processor import HierarchicalSegmentation
from src.core.numeric_analyzer import NumericBaseline
from src.core.llm_analyzer import LLMAnalyzer
from src.core.models import ProjectFeatures

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_integration_test():
    """Run full integration test of the pipeline."""
    print("\n" + "="*60)
    print("INTEGRATION TEST: Complete Pipeline")
    print("="*60)
    
    # Check prerequisites
    if not MONDAY_API_KEY:
        print("✗ MONDAY_API_KEY not set")
        return False
    
    if not os.getenv("OPENAI_API_KEY"):
        print("⚠️  OPENAI_API_KEY not set - LLM analysis will be limited")
    
    # Stage 1: Initialize clients
    print("\n1. Initializing components...")
    monday_client = MondayClient()
    data_extractor = EnhancedMondayExtractor(monday_client)
    segmentation = HierarchicalSegmentation()
    numeric_baseline = NumericBaseline()
    
    # Only initialize LLM if API key is available
    llm_analyzer = None
    if os.getenv("OPENAI_API_KEY"):
        llm_analyzer = LLMAnalyzer()
        print("   ✓ All components initialized")
    else:
        print("   ✓ Components initialized (except LLM)")
    
    # Stage 2: Extract data (small sample)
    print("\n2. Extracting sample data...")
    import src.config
    original_batch = src.config.BATCH_SIZE
    src.config.BATCH_SIZE = 5  # Small sample
    
    df, stats = await data_extractor.extract_all_data(use_cache=True)
    src.config.BATCH_SIZE = original_batch
    
    print(f"   ✓ Extracted {len(df)} records")
    
    # Stage 3: Process and segment data
    print("\n3. Processing data...")
    df_processed = segmentation.process_dataframe(df)
    print(f"   ✓ Processed {len(df_processed)} records")
    
    # Stage 4: Analyze a sample project
    if len(df_processed) > 0:
        print("\n4. Analyzing sample project...")
        
        # Pick first valid project
        sample_row = df_processed.iloc[0]
        
        # Create project features
        project_features = ProjectFeatures(
            project_id=f"TP{sample_row.name:05d}",
            name=sample_row.get('name', ''),
            account=sample_row.get('account'),
            type=sample_row.get('type'),
            category=sample_row.get('category'),
            product_type=sample_row.get('product_type'),
            new_enquiry_value=sample_row.get('new_enquiry_value', 0),
            gestation_period=sample_row.get('gestation_period'),
            pipeline_stage=sample_row.get('pipeline_stage'),
            status_category=sample_row.get('status_category'),
            value_band=sample_row.get('value_band')
        )
        
        # Find segment
        segment, segment_keys = segmentation.find_best_segment(
            sample_row.to_dict(),
            df_processed
        )
        
        print(f"   - Project: {project_features.name}")
        print(f"   - Segment size: {len(segment)} projects")
        print(f"   - Segment keys: {segment_keys}")
        
        # Calculate numeric baselines
        gestation_days, gestation_stats = numeric_baseline.calculate_gestation_baseline(segment)
        conversion_rate, conversion_stats = numeric_baseline.calculate_conversion_rate(segment)
        
        print(f"   - Gestation: {gestation_days} days")
        print(f"   - Conversion: {conversion_rate:.1%}")
        
        # LLM analysis if available
        if llm_analyzer:
            print("\n5. Running LLM analysis...")
            from src.core.models import NumericPredictions, SegmentStatistics
            
            numeric_predictions = NumericPredictions(
                expected_gestation_days=gestation_days or 45,
                expected_conversion_rate=conversion_rate or 0.5,
                rating_score=70
            )
            
            llm_output, metadata = llm_analyzer.analyze_project(
                project_features,
                numeric_predictions
            )
            
            print("   ✓ LLM analysis complete")
            print(f"   - Response time: {metadata.get('response_time', 0):.2f}s")
    
    print("\n" + "="*60)
    print("✓ Integration test COMPLETED successfully")
    print("="*60)
    
    return True


if __name__ == "__main__":
    success = asyncio.run(run_integration_test())
    sys.exit(0 if success else 1)
