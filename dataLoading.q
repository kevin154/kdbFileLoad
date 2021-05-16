
// Load embedPy
\l p.q

// Load utilities for converting pandas dataframes
\l util.q

// Load XML conversion script
\l parsexml.p

// Map parsing function in script to kdb+
parseXML:.p.get`parse_XML


// Import pandas
pd:.p.import`pandas

\d .dl


// Utility to ensure tabular input
checkTabInput:{$[.Q.qt x;0!x;'`$"not table input"]}



// ********
// Parquet
// ********

// Convert kdb+ table to parquet file
tab2pq:{[tab;dir] .ml.tab2df[checkTabInput tab;`:to_parquet;dir]};

// Read parquet file to kdb+ table
pq2tab:{[dir] .ml.df2tab pd[`:read_parquet;dir]};



// ******
// Excel
// ******

// Convert kdb+ table to Excel file
tab2excel:{[tab;dir;sheetName]
  // Ensure sheet name is a string
  if[10h<>type sheetName;
      sheetName:string sheetName
  ];
  // If sheet name unspecified then default to 'Sheet1'
  if[not count sheetName;
      sheetName:"Sheet1"
  ];
  .ml.tab2df[checkTabInput tab;`:to_excel;dir;sheetName;`index pykw 0b]
  };

// Read Excel file to kdb+ tab
excel2tab:{[dir;sheetName]
  if[10h<>type sheetName;
      sheetName:string sheetName
  ];
  // Extract the list of sheet names
  sheets:pd[`:ExcelFile;dir][`:sheet_names]`;
  // If no name specified default to reading first sheet (index 0)
  idx:$[not count sheetName;0;?[sheets;sheetName]];
  // If sheet name specified but not found throw an error
  if[idx=count sheets;
      '`$"invalid sheet name: ", sheetName
  ];
  .ml.df2tab pd[`:read_excel;dir;idx;`engine pykw `openpyxl]
  };



// ********
// Feather
// ********

// Convert kdb+ tab to feather file
tab2feather:{[tab;dir] .ml.tab2df[checkTabInput tab;`:to_feather;dir]};

// Read feather file to kdb+ tab
feather2tab:{[dir] .ml.df2tab pd[`:read_feather;dir]};



// *****
// HDF5
// *****

// Convert kdb+ table to HDF5 file
tab2hdf5:{[tab;dir] .ml.tab2df[checkTabInput tab;`:to_hdf;dir;"df"]};

// Read HDF5 file to kdb+ table
hdf52tab:{[dir] .ml.df2tab pd[`:read_hdf;dir]};



// *******
// Pickle
// *******

// Convert kdb+ table to pickle file
tab2pkl:{[tab;dir] .ml.tab2df[checkTabInput tab;`:to_pickle;dir]};

// Read pickle file to kdb+ table
pkl2tab:{[dir] .ml.df2tab pd[`:read_pickle;dir]};



// ****
// XML
// ****

xml2tab:{[dir;columns] .ml.df2tab parseXML[dir;columns]}



// *****************
// Custom delimiter
// *****************

tab2file:{[tab;dir;delim] .ml.tab2df[checkTabInput tab;`:to_pickle;dir]};

file2tab:{[dir;delim] .ml.tab2df[checkTabInput tab;`:to_pickle;dir]};


\d .