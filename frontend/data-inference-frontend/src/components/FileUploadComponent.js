import React, { useState } from "react";
import axios from "axios";

function FileUploadComponent() {
  const [file, setFile] = useState(null);
  const [inferredTypes, setInferredTypes] = useState(null);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!file) return;

    const formData = new FormData();
    formData.append("file", file);

    axios
      .post("/api/upload/", formData)
      .then((response) => {
        setInferredTypes(response.data.inferred_types);
      })
      .catch((error) => {
        console.error("Error uploading file:", error);
      });
  };

  return (
    <div>
      <form onSubmit={handleSubmit}>
        <input
          type="file"
          accept=".csv, .xls, .xlsx"
          onChange={handleFileChange}
        />
        <button type="submit">Upload and Process</button>
      </form>

      {inferredTypes && (
        <div>
          <h3>Inferred Data Types:</h3>
          <ul>
            {Object.entries(inferredTypes).map(([column, dtype]) => (
              <li key={column}>
                {column}:
                <select defaultValue={dtype}>
                  <option value="Numeric">Numeric</option>
                  <option value="DateTime">DateTime</option>
                  <option value="Boolean">Boolean</option>
                  <option value="Categorical">Categorical</option>
                  <option value="Text">Text</option>
                </select>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

export default FileUploadComponent;
