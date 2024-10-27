import React, { useState } from "react";
import axios from "axios";

function FileUploadComponent() {
  const [file, setFile] = useState(null);
  const [inferredTypes, setInferredTypes] = useState(null);
  const [modifiedTypes, setModifiedTypes] = useState({});
  const [conversionErrors, setConversionErrors] = useState({});
  const [hasHeaders, setHasHeaders] = useState(true);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
    setModifiedTypes({});
  };

  const handleTypeChange = (column, newType) => {
    setModifiedTypes((prev) => ({
      ...prev,
      [column]: newType,
    }));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!file) return;

    const formData = new FormData();
    formData.append("file", file);
    formData.append("has_headers", hasHeaders);

    axios
      .post("/api/upload/", formData)
      .then((response) => {
        setInferredTypes(response.data.inferred_types);
      })
      .catch((error) => {
        console.error("Error uploading file:", error);
      });
  };

  const handleApplyTypes = () => {
    if (!file || Object.keys(modifiedTypes).length === 0) return;

    const formData = new FormData();
    formData.append("file", file);
    formData.append("type_overrides", JSON.stringify(modifiedTypes));

    axios
      .post("/api/upload/", formData)
      .then((response) => {
        setInferredTypes(response.data.inferred_types);
        setConversionErrors({});
        setModifiedTypes({});
      })
      .catch((error) => {
        if (error.response?.data?.conversion_errors) {
          setConversionErrors(error.response.data.conversion_errors);
          setInferredTypes(error.response.data.inferred_types);
        } else {
          console.error("Error applying type changes:", error);
        }
      });
  };

  return (
    <div>
      <form onSubmit={handleSubmit}>
        <div>
          <input
            type="file"
            accept=".csv, .xls, .xlsx"
            onChange={handleFileChange}
          />
          <div>
            <label>
              <input
                type="checkbox"
                checked={hasHeaders}
                onChange={(e) => setHasHeaders(e.target.checked)}
              />
              File has headers
            </label>
          </div>
        </div>
        <button type="submit">Upload and Process</button>
      </form>

      {inferredTypes && (
        <div>
          <h3>Inferred Data Types:</h3>
          <ul>
            {Object.entries(inferredTypes).map(([column, dtype]) => (
              <li key={column}>
                {column}:
                <select
                  value={modifiedTypes[column] || dtype}
                  onChange={(e) => handleTypeChange(column, e.target.value)}
                >
                  <option value="float64">Numeric (float64)</option>
                  <option value="Int64">Numeric (Int64)</option>
                  <option value="datetime64[ns]">Date</option>
                  <option value="bool">Boolean</option>
                  <option value="category">Categorical</option>
                  <option value="object">Text</option>
                  <option value="empty">Empty</option>
                </select>
                {conversionErrors[column] && (
                  <div className="error-message">
                    <p>
                      Error converting to{" "}
                      {conversionErrors[column].requested_type}:
                    </p>
                    <p>{conversionErrors[column].error}</p>
                    <p>
                      Sample values:{" "}
                      {conversionErrors[column].sample_values.join(", ")}
                    </p>
                  </div>
                )}
              </li>
            ))}
          </ul>
          {Object.keys(modifiedTypes).length > 0 && (
            <button onClick={handleApplyTypes}>Apply Type Changes</button>
          )}
        </div>
      )}
    </div>
  );
}

export default FileUploadComponent;
