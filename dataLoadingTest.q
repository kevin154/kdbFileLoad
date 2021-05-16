
// Test data conversion functions using qunit

// Sample data used for testing
testTab:([]sym:`abc`def`ghi;size:1 2 3);

passMsg:{"Correctly returns kdb+ table with expected count for ", x, " file";



// ********
// Parquet
// ********

// Check functions for reading/writing to parquet file
.dl.tab2pq[testTab;"testTab.pq"]

.qunit.assertTrue[count[.dl.pq2tab "testTab.pq"] = count testTab;passMsg "parquet"]



// ******
// Excel
// ******

// Check functions for reading/writing to Excel file
.dl.tab2excel[testTab;"testTab.xlsx";"Sheet1"]

.qunit.assertTrue[count[.dl.excel2tab["testTab.xlsx";""]] = count testTab;passMsg "Excel"]



// *******
// Feather
// *******

// Check functions for reading/writing to Feather file
.dl.tab2feather[testTab;"testTab.feather"]

.qunit.assertTrue[count[.dl.feather2tab "testTab.feather"] = count testTab;passMsg "feather"]



// *****
// HDF5
// *****

// Check functions for reading/writing to HDF5 file
.dl.tab2hdf5[testTab;"testTab.h5"]

.qunit.assertTrue[count[.dl.hdf52tab "testTab.h5"] = count testTab;passMsg "HDF5"]



// *******
// Pickle
// *******

// Check functions for reading/writing to pickle file
.dl.tab2pkl[testTab;"testTab.pkl"]

.qunit.assertTrue[count[.dl.pkl2tab "testTab.pkl"] = count testTab;passMsg "pickle"]



// ****
// XML
// ****

save `testTab.xml;

// Check functions for reading XML file
.qunit.assertTrue[count[.dl.xml2tab["testTab.xml";`sym`size] = count testTab;passMsg "XML"]
