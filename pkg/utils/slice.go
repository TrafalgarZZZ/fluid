package utils

// FillSliceWithString fills a slice with repeated strings
func FillSliceWithString(str string, num int) *[]string {
	retSlice := make([]string, num)

	for i := 0; i < num; i++ {
		retSlice[i] = str
	}

	return &retSlice
}
