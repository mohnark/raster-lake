package store

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// writeDataToSubArray Write pixel data to the array
// Write operations are handled by queries
func writeDataToSubArray(ctx *tiledb.Context, array *tiledb.Array, subarray []int32, pixelData []uint8) error {
	query, err := tiledb.NewQuery(ctx, array)
	if err != nil {
		return fmt.Errorf("Error creating query: %v", err)
	}

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	if err != nil {
		return fmt.Errorf("Error setting query layout: %v", err)
	}

	err = query.SetSubarray(subarray)
	if err != nil {
		return fmt.Errorf("Error setting subarray: %v", err)
	}

	// Returns Buffer Size (ignored for now)
	_, err = query.SetDataBuffer("pixel", pixelData)
	if err != nil {
		return fmt.Errorf("Error setting buffer for pixel data: %v", err)
	}

	err = query.Submit()
	if err != nil {
		return fmt.Errorf("Error submitting query: %v", err)
	}

	return nil
}

// StoreRasterArray Supported 2D Raster Datatypes: UINT 8
func StoreRasterArray(pathURI string, width, height int, pixelData []uint8) error {
	ctx, _ := tiledb.NewContext(nil)

	// Domain with dimensions
	domain, _ := tiledb.NewDomain(ctx)
	xdim, _ := tiledb.NewDimension(ctx, "x", tiledb.TILEDB_INT32, domain, int32(width))
	ydim, _ := tiledb.NewDimension(ctx, "y", tiledb.TILEDB_INT32, domain, int32(height))
	domain.AddDimensions(xdim, ydim)

	// Attr "Pixel" || DN Value of the pixel || Dtype UINT8
	attr, _ := tiledb.NewAttribute(ctx, "pixel", tiledb.TILEDB_UINT8)

	// Dense Schema for Raster Grid like Datasets
	schema, _ := tiledb.NewArraySchema(ctx, tiledb.TILEDB_DENSE)
	schema.SetDomain(domain)
	schema.AddAttributes(attr)

	array, _ := tiledb.NewArray(ctx, pathURI)
	err := array.Create(schema)
	if err != nil {
		return fmt.Errorf("failed to create TileDB array on disk: %w", err)
	}
	err = array.Open(tiledb.TILEDB_WRITE)
	if err != nil {
		return fmt.Errorf("failed to open array for writing: %w", err)
	}
	defer array.Close()

	// Dense subarray to write pixel data to
	subarray := []int32{0, int32(width - 1), 0, int32(height - 1)}
	err = writeDataToSubArray(ctx, array, subarray, pixelData)

	return nil
}
