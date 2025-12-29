"""
PowerMem Script Manager

Provides Python API to manage and execute various maintenance and upgrade scripts.
All script executions require an initialized Memory instance.

Usage example:
    from powermem import Memory
    from scripts.script_manager import ScriptManager
    
    memory = Memory(config={...})
    
    # List available scripts (no need to initialize)
    ScriptManager.list_scripts()
    
    # Execute upgrade script (no need to initialize)
    success = ScriptManager.run('upgrade-sparse-vector', memory)
"""

import importlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ScriptManager:
    """Script Manager - Used to execute maintenance and upgrade scripts"""
    
    _default_config_path = "scripts/scripts_config.json"
    
    @classmethod
    def _load_config(cls) -> Dict[str, Any]:
        """Load script configuration"""
        config_path = Path(cls._default_config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}\n"
                f"Please ensure scripts_config.json exists in the scripts directory"
            )
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration file: {e}")
    
    @classmethod
    def _get_script_info(cls, script_name: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get script information
        
        Args:
            script_name: Script name
            config: Optional config dict, if not provided will load from default path
            
        Returns:
            Script configuration information
            
        Raises:
            ValueError: If script does not exist
        """
        if config is None:
            config = cls._load_config()
        scripts = config.get('scripts', {})
        if script_name not in scripts:
            available = ', '.join(scripts.keys())
            raise ValueError(
                f"Unknown script: {script_name}\n"
                f"Available scripts: {available}\n"
                f"Use ScriptManager.list_scripts() to view details"
            )
        return scripts[script_name]
    
    @classmethod
    def list_scripts(cls, category: Optional[str] = None, verbose: bool = True) -> Dict[str, Dict]:
        """
        List all available scripts
        
        Args:
            category: Optional, only list scripts in the specified category
            verbose: Whether to print to console (default True)
            
        Returns:
            Script dictionary, key is script name, value is script info
        """
        config = cls._load_config()
        scripts = config.get('scripts', {})
        categories = config.get('categories', {})
        
        # Filter by category
        filtered_scripts = {}
        for script_name, script_info in scripts.items():
            if category and script_info.get('category') != category:
                continue
            filtered_scripts[script_name] = script_info
        
        if verbose:
            cls._print_scripts(filtered_scripts, categories)
        
        return filtered_scripts
    
    @classmethod
    def _print_scripts(cls, scripts: Dict[str, Dict], categories: Dict[str, str]) -> None:
        """Print script list"""
        print("\n" + "=" * 70)
        print("PowerMem Available Scripts")
        print("=" * 70)
        
        # Organize scripts by category
        scripts_by_category: Dict[str, list] = {}
        for script_name, script_info in scripts.items():
            script_category = script_info.get('category', 'uncategorized')
            if script_category not in scripts_by_category:
                scripts_by_category[script_category] = []
            scripts_by_category[script_category].append((script_name, script_info))
        
        # Print scripts for each category
        for cat, script_list in sorted(scripts_by_category.items()):
            category_desc = categories.get(cat, cat)
            print(f"\n【{category_desc}】")
            print("-" * 70)
            
            for script_name, script_info in sorted(script_list):
                desc = script_info.get('description', 'No description')
                destructive = script_info.get('destructive', False)
                warning = " ⚠️  Destructive Operation" if destructive else ""
                print(f"  • {script_name}{warning}")
                print(f"    {desc}")
        
        print("\n" + "=" * 70)
        print("Usage: ScriptManager.run('script_name', memory_instance)")
        print("=" * 70 + "\n")
    
    @classmethod
    def run(cls, script_name: str, memory_instance: Any, **kwargs) -> bool:
        """
        Execute the specified script
        
        Args:
            script_name: Script name
            memory_instance: Initialized Memory or AsyncMemory instance
            **kwargs: Additional parameters to pass to the script function
            
        Returns:
            bool: Returns True on success, False on failure
            
        Raises:
            ValueError: If memory_instance is None or script does not exist
            
        Example:
            >>> from powermem import Memory
            >>> from scripts.script_manager import ScriptManager
            >>> 
            >>> memory = Memory(config={...})
            >>> success = ScriptManager.run('upgrade-sparse-vector', memory)
        """
        if memory_instance is None:
            raise ValueError(
                "memory_instance parameter is required\n"
                "Example:\n"
                "  from powermem import Memory\n"
                "  from scripts.script_manager import ScriptManager\n"
                "  memory = Memory(config={{...}})\n"
                "  ScriptManager.run('upgrade-sparse-vector', memory)"
            )
        
        try:
            # Load config and get script information
            config = cls._load_config()
            script_info = cls._get_script_info(script_name, config)
            
            # Display script information
            print(f"\nPreparing to execute script: {script_name}")
            print(f"Description: {script_info.get('description', 'No description')}")
            
            # Check if it's a destructive operation
            if script_info.get('destructive', False):
                print("\n⚠️  Warning: This is a destructive operation that may delete data!")
                confirm = input("Confirm to continue? (yes/no): ").strip().lower()
                if confirm not in ['yes', 'y']:
                    print("Operation cancelled")
                    return False
            
            # Load script module and function
            module_name = script_info['module']
            function_name = script_info['function']
            
            logger.info(f"Loading module: {module_name}")
            module = importlib.import_module(module_name)
            script_func = getattr(module, function_name)
            
            # Execute script
            logger.info(f"Executing script function: {function_name}")
            result = script_func(memory_instance, **kwargs)
            
            if result:
                print(f"\n✓ Script '{script_name}' executed successfully!")
            else:
                print(f"\n✗ Script '{script_name}' execution failed")
            
            return result
            
        except Exception as e:
            logger.error(f"Error occurred while executing script: {e}", exc_info=True)
            print(f"\n✗ Error: {e}")
            return False