"""
Client mapping utilities using client.json for plant and client relationships.
"""

import json
import os
from typing import Dict, List, Optional, Tuple
from backend.logs.logger_setup import setup_logger

# Configure logging
logger = setup_logger('client_mapping', 'client_mapping.log')

def load_client_mapping() -> Dict:
    """
    Load client mapping from client.json file.
    
    Returns:
        Dictionary containing client mapping data
    """
    try:
        # Get the project root directory
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        client_json_path = os.path.join(project_root, 'src', 'client.json')
        
        with open(client_json_path, 'r') as f:
            client_data = json.load(f)
        
        
        return client_data
        
    except Exception as e:
        logger.error(f"Failed to load client mapping: {e}")
        return {}

def get_client_name_from_plant_name(plant_name: str) -> Optional[str]:
    """
    Get client name from plant name using client.json mapping.
    
    Args:
        plant_name: Name of the plant
        
    Returns:
        Client name if found, None otherwise
    """
    try:
        client_data = load_client_mapping()
        
        # Search through both solar and wind sections
        for energy_type in ['solar', 'wind']:
            if energy_type in client_data:
                for client_name, plants in client_data[energy_type].items():
                    for plant in plants:
                        if plant.get('name') == plant_name:
                            logger.info(f"Found client '{client_name}' for plant '{plant_name}'")
                            return client_name
        
        logger.warning(f"No client found for plant '{plant_name}'")
        return None
        
    except Exception as e:
        logger.error(f"Error finding client for plant '{plant_name}': {e}")
        return None

def get_plant_info_from_client_name(client_name: str) -> List[Dict]:
    """
    Get all plant information for a client name.
    
    Args:
        client_name: Name of the client
        
    Returns:
        List of plant dictionaries with 'name' and 'plant_id' keys
    """
    try:
        client_data = load_client_mapping()
        plants = []
        
        # Search through both solar and wind sections
        for energy_type in ['solar', 'wind']:
            if energy_type in client_data:
                if client_name in client_data[energy_type]:
                    plants.extend(client_data[energy_type][client_name])
        
        logger.info(f"Found {len(plants)} plants for client '{client_name}'")
        return plants
        
    except Exception as e:
        logger.error(f"Error finding plants for client '{client_name}': {e}")
        return []

def get_plant_id_from_plant_name(plant_name: str) -> Optional[str]:
    """
    Get plant ID from plant name using client.json mapping.
    
    Args:
        plant_name: Name of the plant
        
    Returns:
        Plant ID if found, None otherwise
    """
    try:
        client_data = load_client_mapping()
        
        # Search through both solar and wind sections
        for energy_type in ['solar', 'wind']:
            if energy_type in client_data:
                for client_name, plants in client_data[energy_type].items():
                    for plant in plants:
                        if plant.get('name') == plant_name:
                            plant_id = plant.get('plant_id')
                            logger.info(f"Found plant_id '{plant_id}' for plant '{plant_name}'")
                            return plant_id
        
        logger.warning(f"No plant_id found for plant '{plant_name}'")
        return None
        
    except Exception as e:
        logger.error(f"Error finding plant_id for plant '{plant_name}': {e}")
        return None

def get_plant_name_from_client_name(client_name: str, energy_type: Optional[str] = None) -> Optional[str]:
    """
    Get the first plant name for a client (useful when client has only one plant).
    
    Args:
        client_name: Name of the client
        energy_type: Optional energy type filter ('solar' or 'wind')
        
    Returns:
        Plant name if found, None otherwise
    """
    try:
        plants = get_plant_info_from_client_name(client_name)
        
        if energy_type:
            # Filter by energy type if specified
            client_data = load_client_mapping()
            if energy_type in client_data and client_name in client_data[energy_type]:
                plants = client_data[energy_type][client_name]
        
        if plants:
            plant_name = plants[0].get('name')
            logger.info(f"Found plant '{plant_name}' for client '{client_name}'")
            return plant_name
        
        logger.warning(f"No plants found for client '{client_name}'")
        return None
        
    except Exception as e:
        logger.error(f"Error finding plant for client '{client_name}': {e}")
        return None

def validate_client_plant_mapping(client_name: str, plant_name: str) -> bool:
    """
    Validate that a client name and plant name are correctly mapped.
    
    Args:
        client_name: Name of the client
        plant_name: Name of the plant
        
    Returns:
        True if mapping is valid, False otherwise
    """
    try:
        plants = get_plant_info_from_client_name(client_name)
        
        for plant in plants:
            if plant.get('name') == plant_name:
                logger.info(f"Validated mapping: client '{client_name}' -> plant '{plant_name}'")
                return True
        
        logger.warning(f"Invalid mapping: client '{client_name}' -> plant '{plant_name}'")
        return False
        
    except Exception as e:
        logger.error(f"Error validating mapping: {e}")
        return False

def get_plant_display_name(plant_name: str) -> str:
    """
    Get display name for a plant (for now, just return the plant name as-is).
    This function can be extended to provide more user-friendly display names.
    
    Args:
        plant_name: Name of the plant
        
    Returns:
        Display name for the plant
    """
    try:
        # For now, just return the plant name
        # In the future, this could be extended to provide more user-friendly names
        if plant_name:
            return str(plant_name)
        return "Unknown Plant"
        
    except Exception as e:
        logger.error(f"Error getting display name for plant '{plant_name}': {e}")
        return plant_name or "Unknown Plant"