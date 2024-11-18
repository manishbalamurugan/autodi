import geopandas as gpd
import pandas as pd
import json
import fiona
import os
from shapely import wkt
from pyproj import Transformer
from shapely.geometry import shape
from typing import Dict, Optional, Union, List

class GeoFileConverter:
    """Utility class for converting various geospatial file formats to CSV."""
    
    def __init__(self, target_crs: str = 'EPSG:4326'):
        """Initialize with target CRS (defaults to WGS84)."""
        self.target_crs = target_crs
        
    def list_gdb_layers(self, gdb_path: str) -> List[str]:
        """List all layers in a geodatabase file."""
        layers = fiona.listlayers(gdb_path)
        print("Available layers:")
        for i, layer in enumerate(layers):
            print(f"{i}: {layer}")
        return layers

    def convert_gdb_to_csv(self, gdb_path: str, output_folder: str) -> None:
        """
        Convert each layer in a GDB file to a CSV with WGS84 coordinates and GeoJSON geometries.
        
        Args:
            gdb_path: Path to the GDB file
            output_folder: Directory to save output CSV files
        """
        os.makedirs(output_folder, exist_ok=True)
        layers = fiona.listlayers(gdb_path)
        print(f"Found {len(layers)} layers in GDB file")
        
        for layer in layers:
            try:
                print(f"\nProcessing layer: {layer}")
                gdf = gpd.read_file(gdb_path, layer=layer)
                
                if 'geometry' not in gdf.columns:
                    print(f"Layer {layer} has no geometry - saving as regular CSV")
                    output_file = os.path.join(output_folder, f"{layer}.csv")
                    gdf.to_csv(output_file, index=False)
                    continue
                
                if gdf.crs and gdf.crs != self.target_crs:
                    gdf = gdf.to_crs(self.target_crs)
                
                gdf['geometry'] = gdf['geometry'].apply(
                    lambda x: json.dumps(x.__geo_interface__) if x is not None else None
                )
                
                output_file = os.path.join(output_folder, f"{layer}.csv")
                gdf.to_csv(output_file, index=False)
                
            except Exception as e:
                print(f"Error processing layer {layer}: {str(e)}")
                continue

    def convert_shapefile_to_csv(self, shp_path: str, output_folder: str) -> None:
        """
        Convert a shapefile to CSV with WGS84 coordinates and GeoJSON geometries.
        
        Args:
            shp_path: Path to the shapefile
            output_folder: Directory to save output CSV file
        """
        os.makedirs(output_folder, exist_ok=True)
        
        try:
            print(f"\nProcessing shapefile: {shp_path}")
            gdf = gpd.read_file(shp_path)
            
            if 'geometry' not in gdf.columns:
                print(f"Shapefile has no geometry - saving as regular CSV")
                output_file = os.path.join(output_folder, f"{os.path.splitext(os.path.basename(shp_path))[0]}.csv")
                gdf.to_csv(output_file, index=False)
                return
            
            if gdf.crs and gdf.crs != self.target_crs:
                gdf = gdf.to_crs(self.target_crs)
            
            gdf['geometry'] = gdf['geometry'].apply(
                lambda x: json.dumps(x.__geo_interface__) if x is not None else None
            )
            
            output_file = os.path.join(output_folder, f"{os.path.splitext(os.path.basename(shp_path))[0]}.csv")
            gdf.to_csv(output_file, index=False)
            
        except Exception as e:
            print(f"Error processing shapefile: {str(e)}")

    def process_geojson(self, file_path: str, output_name: str, init_crs: str,
                       simplify_tolerance: float = 0.001) -> Dict:
        """
        Process GeoJSON file with geometry simplification.
        
        Args:
            file_path: Path to GeoJSON file
            output_name: Output filename 
            init_crs: Initial CRS EPSG code
            simplify_tolerance: Tolerance for geometry simplification (0 to disable)
        """
        gdf = gpd.read_file(file_path)
        gdf = gdf.set_geometry('geometry')
        gdf = gdf[gdf.is_valid]
        gdf = gdf.set_crs(epsg=init_crs, allow_override=True).to_crs(epsg='4326')
        gdf = gdf.reset_index(drop=True)
        
        if simplify_tolerance:
            gdf['geometry'] = gdf['geometry'].simplify(tolerance=simplify_tolerance)
        
        gdf['geometry'] = gdf.geometry.apply(lambda geom: json.dumps(geom.__geo_interface__))
        gdf['DataSource'] = 'GIS'
        csv_data = gdf.to_csv(index=False, quoting=1)
        return {"response": csv_data, "filename": output_name}

    def process_points(self, file_path: str, output_name: str, init_crs: str) -> Dict:
        try:
            print(f"DEBUG: Starting process_points with file: {file_path}")
            
            if file_path.endswith('.geojson'):
                print(f"DEBUG: Reading GeoJSON file")
                gdf = gpd.read_file(file_path)
                print(f"DEBUG: GeoJSON loaded, shape: {gdf.shape}")
                gdf = gdf.set_geometry('geometry')
            else:
                print("DEBUG: Reading CSV file")
                df = pd.read_csv(file_path)
                df['geometry'] = df['geometry'].apply(
                    lambda x: shape(json.loads(x)) if pd.notnull(x) else None
                )
                gdf = gpd.GeoDataFrame(df, geometry='geometry')
            
            print(f"DEBUG: Initial CRS setting to {init_crs}")
            gdf = gdf[gdf.is_valid]
            gdf = gdf.set_crs(epsg=init_crs, allow_override=True).to_crs(epsg='4326')
            gdf = gdf.reset_index(drop=True)
            
            print("DEBUG: Calculating centroids")
            gdf['Longitude'] = gdf.geometry.apply(lambda geom: geom.centroid.x if geom else None)
            gdf['Latitude'] = gdf.geometry.apply(lambda geom: geom.centroid.y if geom else None)
            gdf['DataSource'] = 'GIS'
            gdf = gdf.drop(columns=['geometry'])
            
            print("DEBUG: Converting to CSV")
            csv_data = gdf.to_csv(index=False, quoting=1)
            print(f"DEBUG: CSV data generated, length: {len(csv_data) if csv_data else 'None'}")
            
            return {"response": csv_data, "filename": output_name}
        except Exception as e:
            print(f"DEBUG: Error in process_points: {str(e)}")
            import traceback
            print(f"DEBUG: Full traceback:\n{traceback.format_exc()}")
            return {"response": None, "filename": output_name, "error": str(e)}