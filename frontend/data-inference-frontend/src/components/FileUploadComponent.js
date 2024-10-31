import React, { useState } from "react";
import axios from "axios";

function FileUploadComponent() {
  const [file, setFile] = useState(null);
  const [inferredTypes, setInferredTypes] = useState(null);
  const [modifiedTypes, setModifiedTypes] = useState({});
  const [conversionErrors, setConversionErrors] = useState({});
  const [hasHeaders, setHasHeaders] = useState(true);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);
  const [pageSize, setPageSize] = useState(10);
  const [dataPreview, setDataPreview] = useState(null);

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

    // Reset states on new file upload
    setCurrentPage(1);
    setConversionErrors({});
    setModifiedTypes({});

    const formData = new FormData();
    formData.append("file", file);
    formData.append("has_headers", hasHeaders);
    formData.append("page", 1);
    formData.append("page_size", pageSize);

    axios
      .post("/api/upload/", formData)
      .then((response) => {
        setInferredTypes(response.data.inferred_types);
        setDataPreview(response.data.data_preview);
        setTotalPages(Math.ceil(response.data.total_rows / pageSize));
      })
      .catch((error) => {
        console.error("Error uploading file:", error);
      });
  };

  const handlePageInput = (e) => {
    const value = parseInt(e.target.value);
    if (!isNaN(value) && value > 0 && value <= totalPages) {
      handlePageChange(value);
    }
  };

  const handlePageChange = (newPage) => {
    setCurrentPage(newPage);
    if (!dataPreview) return;

    const formData = new FormData();
    formData.append("page", newPage);
    formData.append("page_size", pageSize);

    axios
      .post("/api/paginate/", formData)
      .then((response) => {
        setDataPreview(response.data.data_preview);
      })
      .catch((error) => {
        console.error("Error fetching page:", error);
      });
  };

  const handleApplyTypes = () => {
    if (!file || Object.keys(modifiedTypes).length === 0) return;

    const formData = new FormData();
    formData.append("file", file);
    formData.append("type_overrides", JSON.stringify(modifiedTypes));
    formData.append("page", currentPage);
    formData.append("page_size", pageSize);

    axios
      .post("/api/upload/", formData)
      .then((response) => {
        setInferredTypes(response.data.inferred_types);
        setDataPreview(response.data.data_preview);
        setTotalPages(Math.ceil(response.data.total_rows / pageSize));
        setConversionErrors({});
        setModifiedTypes({});
      })
      .catch((error) => {
        if (error.response?.data?.conversion_errors) {
          setConversionErrors(error.response.data.conversion_errors);
          setInferredTypes(error.response.data.inferred_types);
          setDataPreview(error.response.data.data_preview);
          setTotalPages(Math.ceil(error.response.data.total_rows / pageSize));
        } else {
          console.error("Error applying type changes:", error);
        }
      });
  };

  const typeOptions = {
    float64: "Numeric (float64)",
    Int64: "Numeric (Int64)",
    "datetime64[ns]": "Date",
    bool: "Boolean",
    category: "Categorical",
    object: "Text",
    string: "Text",
  };

  const getDisplayType = (dtype) => {
    return typeOptions[dtype] || dtype;
  };

  return (
    <div className="file-upload-container">
      <form onSubmit={handleSubmit}>
        <div className="upload-section">
          <input
            type="file"
            accept=".csv, .xls, .xlsx"
            onChange={handleFileChange}
          />
          <div className="header-checkbox">
            <label>
              <input
                type="checkbox"
                checked={hasHeaders}
                onChange={(e) => setHasHeaders(e.target.checked)}
              />
              File has headers
            </label>
          </div>
          <button type="submit">Upload and Process</button>
        </div>
      </form>

      {inferredTypes && (
        <div className="types-container">
          <div className="inferred-types">
            <h3>Inferred Data Types:</h3>
            <table>
              <thead>
                <tr>
                  <th>Column</th>
                  <th>Inferred Type</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(inferredTypes).map(([column, dtype]) => (
                  <tr key={column}>
                    <td>{column}</td>
                    <td>{getDisplayType(dtype)}</td>
                    <td>
                      <button
                        onClick={() => handleTypeChange(column, dtype)}
                        className={modifiedTypes[column] ? "active" : ""}
                      >
                        Modify Type
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {Object.keys(modifiedTypes).length > 0 && (
            <div className="modified-types">
              <h3>Type Modifications:</h3>
              <table>
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Current Type</th>
                    <th>New Type</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(modifiedTypes).map(([column, newType]) => (
                    <tr key={column}>
                      <td>{column}</td>
                      <td>{inferredTypes[column]}</td>
                      <td>
                        <select
                          value={newType}
                          onChange={(e) =>
                            handleTypeChange(column, e.target.value)
                          }
                        >
                          {Object.entries(typeOptions).map(([value, label]) => (
                            <option key={value} value={value}>
                              {value}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td>
                        <button
                          onClick={() => {
                            setModifiedTypes((prev) => {
                              const newTypes = { ...prev };
                              delete newTypes[column];
                              return newTypes;
                            });
                          }}
                          className="remove-btn"
                        >
                          Remove
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <button onClick={handleApplyTypes} className="apply-btn">
                Apply Type Changes
              </button>
            </div>
          )}

          {Object.keys(conversionErrors).length > 0 && (
            <div className="conversion-errors">
              <h3>Conversion Errors:</h3>
              {Object.entries(conversionErrors).map(([column, error]) => (
                <div key={column} className="error-message">
                  <h4>Column: {column}</h4>
                  <p>Failed to convert to {error.requested_type}</p>
                  <p>Error: {error.error}</p>
                  <p>Sample values: {error.sample_values.join(", ")}</p>
                </div>
              ))}
            </div>
          )}

          {dataPreview && (
            <div className="data-preview">
              <h3>Data Preview:</h3>
              <table>
                <thead>
                  <tr>
                    {Object.keys(dataPreview[0] || {}).map((header) => (
                      <th key={header}>{header}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {dataPreview.map((row, idx) => (
                    <tr key={idx}>
                      {Object.values(row).map((value, cellIdx) => (
                        <td key={cellIdx}>{value}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>

              <div className="pagination">
                <button
                  onClick={() => handlePageChange(currentPage - 1)}
                  disabled={currentPage === 1}
                >
                  Previous
                </button>

                <input
                  type="number"
                  value={currentPage}
                  onChange={(e) => handlePageInput(e)}
                  min={1}
                  max={totalPages}
                  className="page-input"
                />
                <span> of {totalPages}</span>

                <button
                  onClick={() => handlePageChange(currentPage + 1)}
                  disabled={currentPage === totalPages}
                >
                  Next
                </button>

                <select
                  value={pageSize}
                  onChange={(e) => {
                    setPageSize(Number(e.target.value));
                    setCurrentPage(1);
                    handlePageChange(1);
                  }}
                >
                  <option value="10">10 rows</option>
                  <option value="25">25 rows</option>
                  <option value="50">50 rows</option>
                  <option value="100">100 rows</option>
                </select>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// Add some basic styling
const styles = `
.file-upload-container {
  padding: 20px;
  max-width: 1200px;
  margin: 0 auto;
}

.upload-section {
  margin-bottom: 20px;
  padding: 15px;
  border: 1px solid #ddd;
  border-radius: 4px;
}

.types-container {
  display: grid;
  gap: 20px;
}

table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 10px;
}

th, td {
  padding: 8px;
  border: 1px solid #ddd;
  text-align: left;
}

th {
  background-color: #f5f5f5;
}

.error-message {
  background-color: #fff3f3;
  border: 1px solid #ffcdd2;
  padding: 10px;
  margin-top: 10px;
  border-radius: 4px;
}

button {
  padding: 6px 12px;
  border-radius: 4px;
  border: 1px solid #ddd;
  background-color: #fff;
  cursor: pointer;
}

button:hover {
  background-color: #f5f5f5;
}

button.active {
  background-color: #e3f2fd;
  border-color: #2196f3;
}

.apply-btn {
  margin-top: 15px;
  background-color: #4caf50;
  color: white;
  border: none;
}

.apply-btn:hover {
  background-color: #45a049;
}

.remove-btn {
  background-color: #f44336;
  color: white;
  border: none;
}

.remove-btn:hover {
  background-color: #d32f2f;
}

select {
  padding: 6px;
  border-radius: 4px;
  border: 1px solid #ddd;
}

select:disabled {
  background-color: #f5f5f5;
  opacity: 1;
  -webkit-text-fill-color: inherit;
  color: inherit;
  cursor: default;
}

.inferred-types select {
  width: 200px;
}

.modified-types select {
  width: 200px;
  font-family: monospace;
}

.data-preview {
  margin-top: 20px;
  overflow-x: auto;
}

.pagination {
  margin-top: 15px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
}

.pagination button {
  padding: 5px 10px;
}

.pagination button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.pagination span {
  margin: 0 10px;
}

.pagination select {
  margin-left: 10px;
}

.page-input {
  width: 60px;
  padding: 4px;
  margin: 0 8px;
  text-align: center;
  border: 1px solid #ddd;
  border-radius: 4px;
}

.page-input::-webkit-inner-spin-button,
.page-input::-webkit-outer-spin-button {
  opacity: 1;
}
`;

// Add styles to document
const styleSheet = document.createElement("style");
styleSheet.innerText = styles;
document.head.appendChild(styleSheet);

export default FileUploadComponent;
