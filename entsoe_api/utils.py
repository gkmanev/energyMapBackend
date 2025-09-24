import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple
from entsoe import EntsoePandasClient
from enum import Enum
from entsoe_api.models import ElectricityLoad, ElectricityPrice, ElectricityGeneration
from django.utils.dateparse import parse_datetime

class DataType(Enum):
    """Enumeration of ENTSO-E data types."""
    ACTUAL_TOTAL_LOAD = "actual_total_load"
    DAY_AHEAD_TOTAL_LOAD_FORECAST = "day_ahead_total_load_forecast"
    INSTALLED_GENERATION_CAPACITY = "installed_generation_capacity"
    ACTUAL_GENERATION = "actual_generation"
    CROSSBORDER_FLOWS = "crossborder_flows"
    DAY_AHEAD_PRICES = "day_ahead_prices"
    GENERATION_FORECAST = "generation_forecast"
    WIND_SOLAR_FORECAST = "wind_solar_forecast"

class EntsoeDataExtractor:
    def __init__(self, api_key: str, country_codes: List[str] = None, 
                 days_back: int = 7, days_forward: int = 1):
        self.client = EntsoePandasClient(api_key=api_key)
        self.country_codes = sorted(country_codes or ["BG"])
        self.days_back = days_back
        self.days_forward = days_forward
        self.failed_extractions = {data_type: [] for data_type in DataType}

    def get_time_period(self) -> Tuple[pd.Timestamp, pd.Timestamp]:
        start = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=self.days_back)
        end = pd.Timestamp.now(tz='UTC') + pd.Timedelta(days=self.days_forward)
        return start, end

    # Corrected methods using actual entsoe-py API
    def query_actual_total_load(self, country_code: str) -> Optional[pd.Series]:
        """Query actual total load data."""
        start, end = self.get_time_period()
        
        try:
            result = self.client.query_load(country_code=country_code, start=start, end=end)
            logging.info(f'Actual load data extracted for country: {country_code}')
            return result
        except Exception as e:
            logging.error(f'Actual load extraction failed for {country_code}: {str(e)}')
            self.failed_extractions[DataType.ACTUAL_TOTAL_LOAD].append(country_code)
            return None

    def query_day_ahead_load_forecast(self, country_code: str) -> Optional[pd.Series]:
        """Query day-ahead total load forecast."""
        start, end = self.get_time_period()
        
        try:
            result = self.client.query_load_forecast(country_code=country_code, start=start, end=end)
            logging.info(f'Load forecast data extracted for country: {country_code}')
            return result
        except Exception as e:
            logging.error(f'Load forecast extraction failed for {country_code}: {str(e)}')
            self.failed_extractions[DataType.DAY_AHEAD_TOTAL_LOAD_FORECAST].append(country_code)
            return None

    def query_installed_capacity(self, country_code: str, psr_type: str = None) -> Optional[pd.DataFrame]:
        """Query installed generation capacity."""
        start, end = self.get_time_period()
        
        try:
            result = self.client.query_installed_generation_capacity(
                country_code=country_code, 
                start=start, 
                end=end, 
                psr_type=psr_type
            )
            logging.info(f'Installed capacity data extracted for country: {country_code}')
            return result
        except Exception as e:
            logging.error(f'Installed capacity extraction failed for {country_code}: {str(e)}')
            self.failed_extractions[DataType.INSTALLED_GENERATION_CAPACITY].append(country_code)
            return None

    def query_actual_generation(self, country_code: str, psr_type: str = None) -> Optional[pd.DataFrame]:
        """Query actual generation data."""
        start, end = self.get_time_period()
        
        try:
            result = self.client.query_generation(
                country_code=country_code, 
                start=start, 
                end=end, 
                psr_type=psr_type
            )
            logging.info(f'Actual generation data extracted for country: {country_code}')
            return result
        except Exception as e:
            logging.error(f'Actual generation extraction failed for {country_code}: {str(e)}')
            self.failed_extractions[DataType.ACTUAL_GENERATION].append(country_code)
            return None

    def query_day_ahead_prices(self, country_code: str) -> Optional[pd.Series]:
        """Query day-ahead electricity prices."""
        start, end = self.get_time_period()
        
        try:
            result = self.client.query_day_ahead_prices(country_code=country_code, start=start, end=end)
            logging.info(f'Day-ahead prices extracted for country: {country_code}')
            return result
        except Exception as e:
            logging.error(f'Day-ahead prices extraction failed for {country_code}: {str(e)}')
            self.failed_extractions[DataType.DAY_AHEAD_PRICES].append(country_code)
            return None

    def query_generation_forecast(self, country_code: str) -> Optional[pd.Series]:
        """Query generation forecast data."""
        start, end = self.get_time_period()
        
        try:
            result = self.client.query_generation_forecast(country_code=country_code, start=start, end=end)
            logging.info(f'Generation forecast extracted for country: {country_code}')
            return result
        except Exception as e:
            logging.error(f'Generation forecast extraction failed for {country_code}: {str(e)}')
            self.failed_extractions[DataType.GENERATION_FORECAST].append(country_code)
            return None

    def query_wind_solar_forecast(self, country_code: str, psr_type: str = None) -> Optional[pd.DataFrame]:
        """Query wind and solar forecast data."""
        start, end = self.get_time_period()
        
        try:
            result = self.client.query_wind_and_solar_forecast(
                country_code=country_code, 
                start=start, 
                end=end, 
                psr_type=psr_type
            )
            logging.info(f'Wind/solar forecast extracted for country: {country_code}')
            return result
        except Exception as e:
            logging.error(f'Wind/solar forecast extraction failed for {country_code}: {str(e)}')
            self.failed_extractions[DataType.WIND_SOLAR_FORECAST].append(country_code)
            return None

    def query_crossborder_flows(self, country_from: str, country_to: str) -> Optional[pd.DataFrame]:
        """Query cross-border electricity flows between countries."""
        start, end = self.get_time_period()
        
        try:
            result = self.client.query_crossborder_flows(
                country_code_from=country_from,
                country_code_to=country_to,
                start=start,
                end=end
            )
            logging.info(f'Cross-border flows extracted: {country_from} -> {country_to}')
            return result
        except Exception as e:
            logging.error(f'Cross-border flows extraction failed {country_from} -> {country_to}: {str(e)}')
            return None

    # Bulk query methods
    def query_all_load_data(self, country_code: str) -> Dict[str, Optional[pd.Series]]:
        """Query both actual load and load forecast for a country."""
        return {
            'actual_load': self.query_actual_total_load(country_code),
            'load_forecast': self.query_day_ahead_load_forecast(country_code)
        }

    def query_all_generation_data(self, country_code: str) -> Dict[str, Optional[pd.DataFrame]]:
        """Query all generation-related data for a country."""
        return {
            'actual_generation': self.query_actual_generation(country_code),
            'installed_capacity': self.query_installed_capacity(country_code),
            'generation_forecast': self.query_generation_forecast(country_code),
            'wind_solar_forecast': self.query_wind_solar_forecast(country_code)
        }

    def query_market_data(self, country_code: str) -> Dict[str, Optional[pd.Series]]:
        """Query market-related data for a country."""
        return {
            'day_ahead_prices': self.query_day_ahead_prices(country_code)      
        }

    # Data processing methods remain the same but corrected for actual return types
    def _process_series_data(self, data: pd.Series, country_code: str, data_type: str) -> List[Dict]:
        """Process Series data (load, prices, etc.)."""
        if data is None:
            return []
            
        data.index = data.index.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ')
        
        data_points = []
        for timestamp, value in data.items():
            if pd.notnull(value):
                data_points.append({
                    'country_code': country_code,
                    'timestamp': timestamp,
                    'data_type': data_type,
                    'value': float(value)
                })
        return data_points

    def _process_generation_data(self, data: pd.DataFrame, country_code: str) -> List[Dict]:
        """Process DataFrame generation data (multiple columns by production type)."""
        if data is None:
            return []
            
        data.index = data.index.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ')
        
        data_points = []
        for timestamp, row in data.iterrows():
            for production_type, value in row.items():
                if pd.notnull(value):
                    data_points.append({
                        'country_code': country_code,
                        'timestamp': timestamp,
                        'production_type': str(production_type).replace("/", "_").replace(" ", "_"),
                        'value': float(value)
                    })
        return data_points

    # Save the data #
    def save_load_data(self, country_code: str, data: pd.Series, data_type: str):
        """Save load data to database."""        
        if data is None:
            return
        
        data.index = data.index.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ')
        
        for timestamp_str, value in data.items():
            if pd.notnull(value):
                timestamp = parse_datetime(timestamp_str)
                ElectricityLoad.objects.update_or_create(
                    country_code=country_code,
                    timestamp=timestamp,
                    data_type=data_type,
                    defaults={'load_mw': float(value)}
                )

    def save_price_data(self, country_code: str, data: pd.Series):
        """Save price data to database."""
        
        if data is None:
            return
        
        data.index = data.index.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ')
        
        for timestamp_str, value in data.items():
            if pd.notnull(value):
                timestamp = parse_datetime(timestamp_str)
                ElectricityPrice.objects.update_or_create(
                    country_code=country_code,
                    timestamp=timestamp,
                    defaults={'price_eur_mwh': float(value)}
                )

    def save_generation_data(self, country_code: str, data: pd.DataFrame, model_class):
        """Save generation or capacity data to database."""
                
        if data is None:
            return
        
        data.index = data.index.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ')
        
        for timestamp_str, row in data.iterrows():
            timestamp = parse_datetime(timestamp_str)
            for production_type, value in row.items():
                if pd.notnull(value):
                    production_type_clean = str(production_type).replace("/", "_").replace(" ", "_")
                    
                    if model_class.__name__ == 'ElectricityGeneration':
                        model_class.objects.update_or_create(
                            country_code=country_code,
                            timestamp=timestamp,
                            production_type=production_type_clean,
                            defaults={'generation_mw': float(value)}
                        )
                    elif model_class.__name__ == 'InstalledCapacity':
                        model_class.objects.update_or_create(
                            country_code=country_code,
                            timestamp=timestamp,
                            production_type=production_type_clean,
                            defaults={'capacity_mw': float(value)}
                        )

    def get_failed_extractions(self, data_type: Optional[DataType] = None) -> Dict:
        """
        Get failed extractions by data type.
        
        Args:
            data_type: Specific data type to get failures for. If None, returns all failures.
            
        Returns:
            Dictionary with data types as keys and failed country lists as values
        """
        if data_type:
            return {data_type: self.failed_extractions[data_type]}
        
        # Return only data types that have failures
        return {dt: failures for dt, failures in self.failed_extractions.items() if failures}

    def reset_failed_extractions(self, data_type: Optional[DataType] = None):
        """
        Reset failed extractions.
        
        Args:
            data_type: Specific data type to reset. If None, resets all.
        """
        if data_type:
            self.failed_extractions[data_type] = []
        else:
            self.failed_extractions = {data_type: [] for data_type in DataType}

    def get_all_failed_extractions(self) -> Dict[DataType, List[str]]:
        """
        Get all failed extractions including empty lists.
        
        Returns:
            Complete dictionary of all data types and their failures
        """
        return self.failed_extractions.copy()

    def has_failures(self) -> bool:
        """
        Check if there are any failed extractions.
        
        Returns:
            True if any extractions failed, False otherwise
        """
        return any(failures for failures in self.failed_extractions.values())

    def get_failure_summary(self) -> Dict[str, int]:
        """
        Get a summary of failure counts by data type.
        
        Returns:
            Dictionary with data type names and failure counts
        """
        return {
            data_type.value: len(failures) 
            for data_type, failures in self.failed_extractions.items() 
            if failures
        }