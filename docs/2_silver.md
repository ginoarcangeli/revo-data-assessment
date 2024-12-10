## Silver Layer

### `load_clean_and_save_silver_data`

**What:**
Loads data from the Bronze Layer, cleans it, fills in missing values, and writes it to the Silver Layer.

**Why:**
The Silver Layer cleans and enriches the data, ensuring it is accurate and complete before further analysis.

**How:**
- Reads the Bronze data using PySpark.
- Cleans and fills missing values using postal code information from a GeoJSON file.
- Writes the cleaned data into the Silver Layer in Parquet format.

---

### `get_postal_code_from_coordinates`

**What:**
Determines the postal code from geographic coordinates using a GeoDataFrame.

**Why:**
This function fills in missing postal codes, which is crucial for accurate geographical analysis.

**How:**
- Uses GeoPandas to iterate through geographic polygons in a GeoDataFrame.
- Identifies the postal code for given latitude and longitude coordinates.

---

### `get_missing_postal_codes`

**What:**
Updates missing postal codes in a DataFrame by geolocating them using a GeoJSON file.

**Why:**
Ensures data completeness by filling in missing geographic information.

**How:**
- Uses a UDF to apply postal code retrieval logic to Spark DataFrame rows.
- Filters out rows with missing postal codes and fills them using latitude and longitude.

---

### `convert_int_to_float`

**What:**
Converts an integer column to a float or double type in the DataFrame.

**Why:**
Facilitates numerical operations that require floating-point precision.

**How:**
- Takes a DataFrame and column names for conversion.
- Uses Spark functions to cast the data type from integer to float/double.

---

### `clean_postal_code_column`

**What:**
Cleans a DataFrame column to retain only valid postal code characters.

**Why:**
Standardizes postal code data to ensure consistency and accuracy.

**How:**
- Uses regex within Spark to remove non-numeric and non-uppercase letter characters.
- Updates or creates a cleaned postal code column.

---

### `clean_and_fill_data`

**What:**
Cleans postal and monetary columns, fills missing data, and drops invalid records.

**Why:**
Prepares data by ensuring all entries are valid and usable for analysis.

**How:**
- Cleans postal and monetary columns, converts monetary values, and fills missing postal codes.
- Utilizes helper functions for cleaning and validation.

---

### `validate_dutch_post_codes`

**What:**
Validates Dutch postal codes, setting invalid entries to null.

**Why:**
Ensures that postal code data conforms to expected Dutch formats for accurate analysis.

**How:**
- Applies a regex pattern to check postal code formats and updates invalid entries.

---

### `clean_and_convert_monetary_col`

**What:**
Cleans and converts a monetary column to a float type.

**Why:**
Ensures monetary data is in a consistent format for numerical analysis.

**How:**
- Strips non-numeric characters using regex, standardizes decimal notation, and converts to float.
