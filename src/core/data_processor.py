"""
Data processing module with label normalization and hierarchical segmentation.
"""

import json
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
from datetime import datetime
import logging

# Use consistent relative imports
try:
    from ..config import (
        PIPELINE_STAGE_LABELS,
        TYPE_LABELS,
        CATEGORY_LABELS,
        MIN_SAMPLE_SIZE,
        CACHE_DIR,
        CACHE_EXPIRY_HOURS
    )
    from .models import ProjectFeatures, StatusCategory
except ImportError:
    # Fallback for when running as standalone script
    from config import (
        PIPELINE_STAGE_LABELS,
        TYPE_LABELS,
        CATEGORY_LABELS,
        MIN_SAMPLE_SIZE,
        CACHE_DIR,
        CACHE_EXPIRY_HOURS
    )
    from models import ProjectFeatures, StatusCategory


logger = logging.getLogger(__name__)


class LabelNormalizer:
    """Parse and apply label mappings from column settings"""
    
    def __init__(self):
        self.pipeline_labels = PIPELINE_STAGE_LABELS
        self.type_labels = TYPE_LABELS
        self.category_labels = CATEGORY_LABELS
        
    def extract_labels(self, settings_str: str) -> Dict[str, str]:
        """Extract label mappings from settings_str JSON"""
        try:
            settings = json.loads(settings_str)
            labels = settings.get('labels', {})
            # Convert to string keys for consistency
            return {str(k): v for k, v in labels.items()}
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Failed to parse settings_str: {settings_str}")
            return {}
    
    def normalize_pipeline_stage(self, stage_value: Any) -> Tuple[str, str]:
        """
        Map pipeline stage to Win/Lost/Open categories
        Returns: (category, original_label)
        """
        # Convert to string for lookup
        stage_key = str(stage_value) if stage_value is not None else ""
        label = self.pipeline_labels.get(stage_key, "Unknown")
        
        # Categorize into Closed-Won vs Non-Closed
        if label == "Won - Closed (Invoiced)":
            return StatusCategory.WON.value, label
        elif label == "Lost":
            return StatusCategory.LOST.value, label
        else:
            return StatusCategory.OPEN.value, label
    
    def normalize_type(self, type_value: Any) -> str:
        """Normalize type dropdown value"""
        type_key = str(type_value) if type_value is not None else ""
        return self.type_labels.get(type_key, type_value if type_value else "Unknown")
    
    def normalize_category(self, category_value: Any) -> str:
        """Normalize category dropdown value"""
        cat_key = str(category_value) if category_value is not None else ""
        return self.category_labels.get(cat_key, category_value if category_value else "Unknown")

    def normalize_column_value(self, column_value: Dict, column_id: str) -> Optional[Any]:
        """
        Normalize a column value based on its type and ID
        
        Args:
            column_value: Column value dict with 'text', 'value', 'type', etc.
            column_id: The column ID to determine normalization strategy
            
        Returns:
            Normalized value or None if cannot be normalized
        """
        if not column_value:
            return None
            
        # Get the raw value and text
        text = column_value.get('text', '')
        value = column_value.get('value')
        col_type = column_value.get('type', '')
        
        # Prefer display_value for formula/mirror BEFORE empty checks
        if col_type == 'formula':
            dv = column_value.get('display_value')
            if dv is not None and str(dv).strip() and str(dv).lower() not in ('null','none'):
                return self._parse_numeric_value(dv)
        if col_type == 'mirror':
            dv = column_value.get('display_value')
            if dv is not None and str(dv).strip() and str(dv).lower() not in ('null','none'):
                return dv
        
        # Handle empty values (non-formula/mirror)
        if not text and not value:
            return None
            
        # Column-specific normalization
        # Generic dropdowns: return their label text
        if col_type == 'dropdown':
            return text if text else None

        # Numbers
        if col_type == 'numbers':
            return self._parse_numeric_value(text or value)

        # Formula fallback
        if col_type == 'formula':
            return self._parse_numeric_value(text or value)

        # Dates
        if col_type == 'date':
            return text if text else None

        # Mirror fallback
        if col_type == 'mirror':
            return text if text else None

        # Default
        return text if text else str(value) if value else None


        
        # if column_id in ['dropdown', 'dropdown2__1']:  # Pipeline stage
        # if column_id == 'status4__1':  # Pipeline stage
        #     return self.normalize_pipeline_stage(text or value)[1]  # Return label, not category
        # elif column_id in ['dropdown3', 'dropdown3__1']:  # Type
        #     return self.normalize_type(text or value)
        # elif column_id in ['dropdown4', 'dropdown4__1']:  # Category  
        #     return self.normalize_category(text or value)
        # elif col_type == 'numbers':  # Numeric columns
        #     return self._parse_numeric_value(text or value)
        # elif col_type == 'formula':  # Fallback formula handling
        #     return self._parse_numeric_value(text or value)
        # elif col_type == 'date':  # Date values
        #     return text if text else None
        # elif col_type == 'mirror':  # Fallback mirror handling
        #     return text
        # else:
        #     # Default: return text value
        #     return text if text else str(value) if value else None
    
    def _parse_numeric_value(self, value: Any) -> Optional[float]:
        """Parse numeric values with proper error handling"""
        if value is None or value == '':
            return None
        
        try:
            if isinstance(value, str):
                # Remove currency symbols and parse
                clean_value = value.replace('£', '').replace(',', '').strip()
                return float(clean_value) if clean_value else None
            return float(value)
        except (ValueError, TypeError):
            return None


class HierarchicalSegmentation:
    """
    Implements hierarchical backoff for sparse data segments
    """
    
    def __init__(self, min_sample_size: int = MIN_SAMPLE_SIZE):
        self.min_sample_size = min_sample_size
        self.backoff_tiers = [
            ['account', 'type', 'category', 'product_type', 'value_band'],  # Tier 1: All features
            ['type', 'category', 'product_type', 'value_band'],              # Tier 2: Drop account
            ['category', 'product_type', 'type'],                            # Tier 3: Drop value
            ['category', 'type'],                                            # Tier 4: Core duo
            []                                                                # Tier 5: Global
        ]
        
    def create_value_bands(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create quartile-based value bands"""
        # Handle missing or zero values
        values = df['new_enquiry_value'].fillna(0)
        
        # Only create bands if we have non-zero values
        non_zero_values = values[values > 0]
        
        if len(non_zero_values) >= 4:
            try:
                df['value_band'] = pd.qcut(
                    values,
                    q=4,
                    labels=['Q1 (0-25%)', 'Q2 (25-50%)', 'Q3 (50-75%)', 'Q4 (75-100%)'],
                    duplicates='drop'
                )
            except Exception as e:
                logger.warning(f"Failed to create quartile bands: {e}. Using fallback method.")
                df['value_band'] = self._create_fallback_bands(values)
        else:
            df['value_band'] = self._create_fallback_bands(values)
            
        return df
    
    def _create_fallback_bands(self, values: pd.Series) -> pd.Series:
        """Create simple value bands when quartiles fail"""
        conditions = [
            values <= 0,
            (values > 0) & (values <= 15000),
            (values > 15000) & (values <= 40000),
            (values > 40000) & (values <= 100000),
            values > 100000
        ]
        choices = ['Zero', 'Small (<15k)', 'Medium (15-40k)', 'Large (40-100k)', 'XLarge (>100k)']
        return pd.Series(np.select(conditions, choices, default='Unknown'), index=values.index)
    
    def find_best_segment(
        self, 
        target_project: Dict[str, Any], 
        historical_data: pd.DataFrame
    ) -> Tuple[pd.DataFrame, List[str], int]:
        """
        Find the most specific segment with sufficient data
        Returns: (segment_data, tier_keys_used, backoff_tier)
        """
        for tier_idx, tier_keys in enumerate(self.backoff_tiers):
            if not tier_keys:  # Global fallback
                segment = historical_data
                logger.info(f"Using global segment (Tier {tier_idx}): {len(segment)} projects")
            else:
                # Build filter conditions
                conditions = []
                matched_keys = []
                
                for key in tier_keys:
                    if key in target_project and target_project[key] is not None:
                        # Handle value_band specially as it might be a category
                        if key == 'value_band' and hasattr(target_project[key], 'cat'):
                            conditions.append(historical_data[key] == target_project[key])
                        else:
                            conditions.append(historical_data[key] == target_project[key])
                        matched_keys.append(key)
                
                if conditions:
                    try:
                        segment = historical_data[np.logical_and.reduce(conditions)]
                        logger.info(
                            f"Tier {tier_idx} segment using {matched_keys}: "
                            f"{len(segment)} projects"
                        )
                    except Exception as e:
                        logger.warning(f"Error filtering at tier {tier_idx}: {e}")
                        continue
                else:
                    continue
            
            if len(segment) >= self.min_sample_size:
                return segment, tier_keys if tier_keys else [], tier_idx
        
        # Return global if all else fails
        logger.warning(
            f"All tiers had insufficient data. Using global segment: "
            f"{len(historical_data)} projects"
        )
        return historical_data, [], len(self.backoff_tiers) - 1


class MirrorResolver:
    """
    Resolves mirror columns by reading from subitem sources directly
    instead of relying on parent board mirror rendering
    """
    
    def resolve_mirrors(
        self, 
        parent_items: List[Dict], 
        subitems: List[Dict]
    ) -> List[Dict]:
        """
        Map subitem values to parent items through mirror relationships
        """
        # Group subitems by parent
        subitems_by_parent = {}
        orphaned_subitems = 0
        total_subitems_processed = 0
        
        for subitem in subitems:
            total_subitems_processed += 1
            # Fix: Handle None or missing parent_item properly
            parent_item = subitem.get('parent_item')
            
            # Check if parent_item exists and is a dict
            if parent_item and isinstance(parent_item, dict):
                parent_id = parent_item.get('id')
            else:
                parent_id = None
                orphaned_subitems += 1
            
            if parent_id:
                if parent_id not in subitems_by_parent:
                    subitems_by_parent[parent_id] = []
                subitems_by_parent[parent_id].append(subitem)
        
        logger.info(f"Processed {total_subitems_processed} subitems")
        logger.info(f"Found {len(subitems_by_parent)} parent-subitem relationships")
        
        if orphaned_subitems > 0:
            logger.warning(f"Found {orphaned_subitems} subitems without parent references")
        
        # Track resolution statistics
        resolved_count = 0
        
        # Resolve mirror values for each parent
        for parent in parent_items:
            parent_id = parent.get('id')
            if not parent_id:
                logger.warning(f"Parent item missing ID: {parent}")
                parent['account'] = None
                parent['product_type'] = None
                parent['new_enquiry_value'] = 0
                continue
                
            parent_subitems = subitems_by_parent.get(parent_id, [])
            
            if not parent_subitems:
                parent['account'] = None
                parent['product_type'] = None
                parent['new_enquiry_value'] = 0
                continue
            
            # Aggregate Account (most common)
            accounts = []
            for s in parent_subitems:
                account_val = self._extract_column_value(s, 'mirror_12__1')
                if account_val:
                    accounts.append(account_val)
            
            parent['account'] = max(set(accounts), key=accounts.count) if accounts else None
            
            # Aggregate Product Type (most common)
            products = []
            for s in parent_subitems:
                product_val = self._extract_column_value(s, 'mirror875__1')
                if product_val:
                    products.append(product_val)
            
            parent['product_type'] = max(set(products), key=products.count) if products else None
            
            # Sum New Enquiry Values
            values = []
            for s in parent_subitems:
                value = self._extract_numeric_value(s, 'formula_mkqa31kh')
                if value is not None and value > 0:
                    values.append(value)
            
            parent['new_enquiry_value'] = sum(values) if values else 0
            
            # Count if we resolved any data
            if parent['account'] or parent['product_type'] or parent['new_enquiry_value'] > 0:
                resolved_count += 1
        
        logger.info(f"Resolved mirrors for {len(parent_items)} parent items")
        logger.info(f"Successfully resolved data for {resolved_count} items ({resolved_count/len(parent_items)*100:.1f}%)")
        
        return parent_items
    
    def _extract_column_value(self, item: Dict, column_id: str) -> Optional[str]:
        """Enhanced column value extraction with multiple fallbacks"""
        column_values = item.get('column_values', [])
        for col in column_values:
            if col.get('id') == column_id:
                # Priority 1: display_value for mirror/formula columns
                if col.get('type') in ['mirror', 'formula']:
                    display_value = col.get('display_value')
                    if display_value is not None and str(display_value).strip():
                        return str(display_value).strip()
                
                # Priority 2: text value
                text = col.get('text')
                if text is not None and str(text).strip():
                    return str(text).strip()
                
                # Priority 3: raw value (parse if JSON)
                value = col.get('value')
                if value is not None:
                    # Handle JSON values
                    if isinstance(value, str) and value.startswith('{'):
                        try:
                            parsed = json.loads(value)
                            # Extract text from complex values
                            if isinstance(parsed, dict):
                                return parsed.get('text', str(value))
                        except json.JSONDecodeError:
                            pass
                    return str(value).strip() if str(value).strip() else None
        
        logger.debug(f"No value found for column {column_id} in item {item.get('id')}")
        return None
    
    def _extract_numeric_value(self, item: Dict, column_id: str) -> Optional[float]:
        """Extract numeric value from a column"""
        column_values = item.get('column_values', [])
        for col in column_values:
            if col.get('id') == column_id:
                # Priority 1: display_value for formula columns
                if col.get('type') == 'formula':
                    display_value = col.get('display_value')
                    if display_value is not None and str(display_value).strip():
                        try:
                            # Remove currency symbols and parse
                            clean_value = str(display_value).replace('£', '').replace(',', '').strip()
                            return float(clean_value) if clean_value else None
                        except (ValueError, TypeError):
                            pass
                
                # Priority 2: text value (fallback)
                text = col.get('text')
                if text is not None:
                    text = text.strip()
                    if text:
                        try:
                            # Remove currency symbols and parse
                            text = text.replace('£', '').replace(',', '').strip()
                            return float(text)
                        except (ValueError, TypeError):
                            pass
        return None


class EnhancedMirrorResolver(MirrorResolver):
    """Enhanced mirror resolver that uses hidden items as source of truth"""
    
    def resolve_mirrors_with_hidden_items(
        self, 
        parent_items: List[Dict], 
        subitems: List[Dict],
        hidden_items: List[Dict]
    ) -> List[Dict]:
        """
        Resolve mirrors using direct hidden items data instead of broken mirrors
        """
        # Create lookup for hidden items by name
        hidden_items_by_name = {}
        for hidden_item in hidden_items:
            name = hidden_item.get('name', '')
            hidden_items_by_name[name] = hidden_item
        
        # Group subitems by parent
        subitems_by_parent = {}
        for subitem in subitems:
            parent_item = subitem.get('parent_item')
            if parent_item and isinstance(parent_item, dict):
                parent_id = parent_item.get('id')
                if parent_id:
                    if parent_id not in subitems_by_parent:
                        subitems_by_parent[parent_id] = []
                    subitems_by_parent[parent_id].append(subitem)
        
        # Resolve values for each parent
        for parent in parent_items:
            parent_id = parent.get('id')
            parent_subitems = subitems_by_parent.get(parent_id, [])
            
            accounts = []
            products = []
            total_new_enquiry_value = 0
            design_dates = []
            invoice_dates = []
            
            for subitem in parent_subitems:
                subitem_name = subitem.get('name', '')
                
                # Get account and product type using display_value
                account_val = self._extract_column_value(subitem, 'mirror_12__1')
                if account_val:
                    accounts.append(account_val)
                
                product_val = self._extract_column_value(subitem, 'mirror875__1')
                if product_val:
                    products.append(product_val)
                
                # Get data from hidden items
                hidden_item = hidden_items_by_name.get(subitem_name)
                if hidden_item:
                    # New Enquiry Value
                    reason = self._extract_column_value(hidden_item, 'dropdown2__1')
                    if reason == "New Enquiry":
                        quote_amount = self._extract_numeric_value(hidden_item, 'formula63__1')
                        if quote_amount and quote_amount > 0:
                            total_new_enquiry_value += quote_amount
                    
                    # Gestation Period dates
                    design_date = self._extract_date_value(hidden_item, 'date__1')
                    if design_date:
                        design_dates.append(design_date)
                    
                    invoice_date = self._extract_date_value(hidden_item, 'date42__1')
                    if invoice_date:
                        invoice_dates.append(invoice_date)
            
            # Set aggregated values
            parent['account'] = max(set(accounts), key=accounts.count) if accounts else None
            parent['product_type'] = max(set(products), key=products.count) if products else None
            parent['new_enquiry_value'] = total_new_enquiry_value
            
            # Calculate gestation period
            parent['gestation_period'] = self._calculate_gestation_period(design_dates, invoice_dates)
        
        return parent_items
    
    def _extract_date_value(self, item: Dict, column_id: str) -> Optional[str]:
        """Extract date value from a column"""
        column_values = item.get('column_values', [])
        for col in column_values:
            if col.get('id') == column_id:
                text = col.get('text')
                if text and text.strip():
                    return text.strip()
        return None
    
    def _calculate_gestation_period(self, design_dates: List[str], invoice_dates: List[str]) -> Optional[int]:
        """Calculate gestation period from design and invoice dates"""
        # If no design dates, cannot compute
        if not design_dates:
            return None
        # If design exists but invoice missing → Monday formula returns 0
        if not invoice_dates:
            return 0
        
        try:
            from datetime import datetime
            
            # Get earliest design date and earliest invoice date
            earliest_design = min(design_dates)
            earliest_invoice = min(invoice_dates)
            
            # Parse dates
            design_dt = datetime.strptime(earliest_design, '%Y-%m-%d')
            invoice_dt = datetime.strptime(earliest_invoice, '%Y-%m-%d')
            
            # Calculate days difference
            days_diff = (invoice_dt - design_dt).days
            
            # Bound per spec: invalid/huge → 0
            if days_diff > 500000 or days_diff < 0:
                return 0
            
            return days_diff
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to calculate gestation period: {e}")
            return None

class CacheManager:
    """Manage caching for API responses and computed metrics"""
    
    def __init__(self, cache_dir: Path = CACHE_DIR):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True)
    
    def cache_label_mappings(self, board_id: str, column_id: str, labels: Dict):
        """Cache column label mappings"""
        cache_file = self.cache_dir / f"labels_{board_id}_{column_id}.json"
        with open(cache_file, 'w') as f:
            json.dump({
                'labels': labels,
                'cached_at': datetime.now().isoformat()
            }, f)
    
    def get_cached_labels(self, board_id: str, column_id: str) -> Optional[Dict]:
        """Retrieve cached labels if fresh"""
        cache_file = self.cache_dir / f"labels_{board_id}_{column_id}.json"
        if cache_file.exists():
            with open(cache_file, 'r') as f:
                data = json.load(f)
                cached_time = datetime.fromisoformat(data['cached_at'])
                hours_elapsed = (datetime.now() - cached_time).total_seconds() / 3600
                if hours_elapsed < CACHE_EXPIRY_HOURS:
                    return data['labels']
        return None
    
    def cache_processed_data(self, data: pd.DataFrame, filename: str):
        """Cache processed dataframe"""
        cache_file = self.cache_dir / f"{filename}.pkl"
        data.to_pickle(cache_file)
        
        # Also save metadata
        meta_file = self.cache_dir / f"{filename}_meta.json"
        with open(meta_file, 'w') as f:
            json.dump({
                'cached_at': datetime.now().isoformat(),
                'shape': list(data.shape),
                'columns': list(data.columns)
            }, f)
    
    def get_cached_data(self, filename: str) -> Optional[pd.DataFrame]:
        """Retrieve cached dataframe if fresh"""
        cache_file = self.cache_dir / f"{filename}.pkl"
        meta_file = self.cache_dir / f"{filename}_meta.json"
        
        if cache_file.exists() and meta_file.exists():
            with open(meta_file, 'r') as f:
                meta = json.load(f)
                cached_time = datetime.fromisoformat(meta['cached_at'])
                hours_elapsed = (datetime.now() - cached_time).total_seconds() / 3600
                if hours_elapsed < CACHE_EXPIRY_HOURS:
                    return pd.read_pickle(cache_file)
        return None
