package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

type rtrResponse struct {
	Message   string
	Status    uint16
	Timestamp string
	Data      []transaction
	Version   rtrVersion
}

type rtrVersion struct {
	Id        uint16
	Published string
}

type transaction struct {
	Rechtstraeger string
	Quartal       string
	Bekanntgabe   uint8
	Medieninhaber string `json:"mediumMedieninhaber"`
	Euro          float32
}

type feature interface {
	Run()
}

type noopFeature struct {
}

func (nop *noopFeature) Run() {
	fmt.Println("Unkown Command")
}

type helpFeature struct {
}

func (help *helpFeature) Run() {
	fmt.Println("\tquit | exit ... quits the program")
	fmt.Println("\thelp ... shows this message")
	fmt.Println("\tpayers ... prints a sorted list of all payers")
	fmt.Println("\trecipients ... prints a sorted list of all recipients")
	fmt.Println("\tquarters ... prints a list of loaded quarters")
	fmt.Println("\ttop n <payers|recipients> <§2|§4|§31> ... prints the top n payers/recipients for given paragraph")
	fmt.Println("\tsearch <payers|recipients> searchTerm ... prints a list of payers/recipients containing the given search term")
	fmt.Println("\tdetails <payers|recipients> organization ... prints a list of all payments payed or received by the given payer/recipient")
}

type exitFeature struct {
	isRunning *bool
}

func (exit *exitFeature) Run() {
	*exit.isRunning = false
}

func lessLower(sa, sb string) bool {
	for {
		rb, nb := utf8.DecodeRuneInString(sb)
		if nb == 0 {
			return false
		}

		ra, na := utf8.DecodeRuneInString(sa)
		if na == 0 {
			return true
		}

		rb = unicode.ToLower(rb)
		ra = unicode.ToLower(ra)

		if ra != rb {
			return ra < rb
		}

		sa = sa[na:]
		sb = sb[nb:]
	}
}

func printUniqueResults(mapper func(transaction) string, transactions []transaction) {
	set := make(map[string]struct{})
	for _, transaction := range transactions {
		s := mapper(transaction)
		if _, exists := set[s]; !exists {
			set[s] = struct{}{}
		}
	}
	keys := make([]string, len(set))
	i := 0
	for k := range set {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool { return lessLower(keys[i], keys[j]) })
	for _, s := range keys {
		fmt.Printf("\t%s\n", s)
	}
}

type baseFeature struct {
	transactions []transaction
}

type payersFeature struct {
	base baseFeature
}

func getRechtstraeger(t transaction) string { return t.Rechtstraeger }

func getMedieninhaber(t transaction) string { return t.Medieninhaber }

func getQuartal(t transaction) string { return t.Quartal }

func (payers *payersFeature) Run() {
	printUniqueResults(getRechtstraeger, payers.base.transactions)
}

type recipientsFeature struct {
	base baseFeature
}

func (recipients *recipientsFeature) Run() {
	printUniqueResults(getMedieninhaber, recipients.base.transactions)
}

type quartersFeature struct {
	base baseFeature
}

func (quarters *quartersFeature) Run() {
	printUniqueResults(getQuartal, quarters.base.transactions)
}

func isValidQuarter(quarter string) bool {
	if len(quarter) != 5 {
		return false
	}
	for _, char := range quarter {
		if !unicode.IsDigit(rune(char)) {
			return false
		}
	}
	return true
}

func loadData(quarter string) ([]transaction, error) {
	if !isValidQuarter(quarter) {
		return nil, fmt.Errorf("%s is not a valid quarter", quarter)
	}
	fmt.Printf("Loading data for quarter %s\n", quarter)
	resp, err := http.Get(fmt.Sprintf("https://data.rtr.at/api/v1/tables/MedKFTGBekanntgabe.json?quartal=%s&leermeldung=0&size=0", quarter))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result rtrResponse
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func loadMultipleData(quarters []string, data *[]transaction) error {
	if len(quarters) == 0 {
		return fmt.Errorf("at least one quarter to be loaded must be specified")
	}
	errs := make([]error, 0)
	var errorsMutex sync.Mutex
	var transactionMutex sync.Mutex
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(quarters))
	for _, quarter := range quarters {
		go func() {
			defer waitGroup.Done()
			transactions, err := loadData(quarter)
			if err != nil {
				errorsMutex.Lock()
				errs = append(errs, err)
				errorsMutex.Unlock()
				return
			}
			transactionMutex.Lock()
			defer transactionMutex.Unlock()
			*data = append(*data, transactions...)
		}()
	}
	waitGroup.Wait()

	return errors.Join(errs...)
}

type loadFeature struct {
	quarters []string
	data     *[]transaction
}

func (load *loadFeature) Run() {
	err := loadMultipleData(load.quarters, load.data)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
}

type reloadFeature struct {
	base loadFeature
}

func (reload *reloadFeature) Run() {
	clear(*reload.base.data)
	reload.base.Run()
}

type dataFeature struct {
	arguments []string
	data      []transaction
}

type topFeature struct {
	base dataFeature
}

type stringFloatPair struct {
	str   string
	value float64
}

func (top *topFeature) Run() {
	if len(top.base.arguments) != 3 {
		fmt.Println("Wrong syntax for command top")
		return
	}
	amount, err := strconv.ParseUint(top.base.arguments[0], 10, 8)
	if err != nil {
		fmt.Printf("Value %s for parameter 1 is invalid: %v\n", top.base.arguments[0], err)
		return
	}

	var mapper func(transaction) string
	switch strings.ToLower(top.base.arguments[1]) {
	case "payers":
		mapper = getRechtstraeger
	case "recipients":
		mapper = getMedieninhaber
	default:
		fmt.Printf("Value %s for parameter 2 is invalid: Allowed values are ['payers', 'recipients']\n", top.base.arguments[1])
		return
	}

	bekanntgabe, err := strconv.ParseUint(top.base.arguments[2], 10, 8)
	if err != nil {
		fmt.Printf("Value %s for parameter 3 is invalid: %v\n", top.base.arguments[2], err)
		return
	}
	if amount <= 0 {
		fmt.Printf("%d is not a valid input for parameter amount\n", amount)
		return
	}
	if bekanntgabe != 2 && bekanntgabe != 4 && bekanntgabe != 31 {
		fmt.Printf("%d is not a valid input for parameter bekanntgabe\n", bekanntgabe)
		return
	}
	m := make(map[string]float64)
	for _, transaction := range top.base.data {
		if transaction.Bekanntgabe != uint8(bekanntgabe) {
			continue
		}
		key := mapper(transaction)
		if euro, exists := m[key]; exists {
			m[key] = euro + float64(transaction.Euro)
		} else {
			m[key] = float64(transaction.Euro)
		}
	}
	slice := make([]stringFloatPair, 0, len(m))
	for key, value := range m {
		slice = append(slice, stringFloatPair{str: key, value: value})
	}
	sort.Slice(slice, func(i, j int) bool { return slice[i].value > slice[j].value })
	maxFloatWidth := int(math.Log10(slice[0].value)) + 1
	maxStringLength := 0
	for _, pair := range slice {
		length := len(pair.str)
		if length > maxStringLength {
			maxStringLength = length
		}
	}

	for idx, pair := range slice[0:amount] {
		fmt.Printf(("\t%3d. %" + strconv.Itoa(maxStringLength) + "s - %" + strconv.Itoa(maxFloatWidth) + ".2f€\n"), idx+1, pair.str, pair.value)
	}
}

type searchFeature struct {
	base dataFeature
}

func (search searchFeature) Run() {
	if len(search.base.arguments) < 2 {
		fmt.Println("At least two parameters need to be provided")
		return
	}
	por := search.base.arguments[0]
	var mapper func(transaction) string
	switch por {
	case "payers":
		mapper = getRechtstraeger
	case "recipients":
		mapper = getMedieninhaber
	default:
		fmt.Printf("Value %s for parameter 2 is invalid: Allowed values are ['payers', 'recipients']\n", search.base.arguments[0])
	}
	searchTerm := strings.ToLower(strings.Join(search.base.arguments[1:], " "))
	set := make(map[string]struct{})
	for _, transaction := range search.base.data {
		str := mapper(transaction)
		if strings.Contains(strings.ToLower(str), searchTerm) {
			set[str] = struct{}{}
		}
	}
	slice := make([]string, 0, len(set))
	for str := range set {
		slice = append(slice, str)
	}
	sort.Slice(slice, func(i, j int) bool { return lessLower(slice[i], slice[j]) })
	for idx, str := range slice {
		fmt.Printf("\t%d. %s\n", idx+1, str)
	}
}

type detailsFeature struct {
	base dataFeature
}

func printAll(m map[string]float64) {
	length := len(m)

	if length == 0 {
		return
	}

	slice := make([]stringFloatPair, 0, length)
	for key, value := range m {
		slice = append(slice, stringFloatPair{str: key, value: value})
	}
	sort.Slice(slice, func(i, j int) bool { return slice[i].value > slice[j].value })
	maxFloatWidth := int(math.Log10(slice[0].value)) + 1
	maxStringLength := 0
	for _, pair := range slice {
		length := len(pair.str)
		if length > maxStringLength {
			maxStringLength = length
		}
	}

	for idx, pair := range slice {
		fmt.Printf(("\t%3d. %" + strconv.Itoa(maxStringLength) + "s - %" + strconv.Itoa(maxFloatWidth) + ".2f€\n"), idx+1, pair.str, pair.value)
	}
}

func (details detailsFeature) Run() {
	if len(details.base.arguments) < 1 {
		fmt.Println("At least one parameter needs to be provided")
		return
	}
	por := details.base.arguments[0]
	var mapper func(transaction) string
	var reverseMapper func(transaction) string
	switch por {
	case "payers":
		mapper = getRechtstraeger
		reverseMapper = getMedieninhaber
	case "recipients":
		mapper = getMedieninhaber
		reverseMapper = getRechtstraeger
	default:
		fmt.Printf("Value %s for parameter 2 is invalid: Allowed values are ['payers', 'recipients']\n", details.base.arguments[0])
		return
	}
	organization := strings.Join(details.base.arguments[1:], " ")
	m := make(map[uint8]map[string]float64)
	m[2] = make(map[string]float64)
	m[4] = make(map[string]float64)
	m[31] = make(map[string]float64)
	for _, transaction := range details.base.data {
		if mapper(transaction) == organization {
			a := m[transaction.Bekanntgabe]
			a[reverseMapper(transaction)] += float64(transaction.Euro)
		}
	}
	fmt.Println("\tPayments §2:")
	printAll(m[2])
	fmt.Println("\tPayments §4:")
	printAll(m[4])
	fmt.Println("\tPayments §31:")
	printAll(m[31])
}

func main() {

	args := os.Args[1:]

	data := make([]transaction, 0)
	err := loadMultipleData(args, &data)
	if err != nil {
		fmt.Printf("An error occured while loading data: %v\n", err)
	}

	fmt.Println("Welcome to the Go-Microproject!")
	reader := bufio.NewReader(os.Stdin)
	isRunning := true
	for isRunning {
		fmt.Println("Please enter a command or type 'help' for more information")
		fmt.Print("> ")
		text, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("An error occured: %v\n", err)
			continue
		}
		text = strings.Trim(text, " \t\n")
		cli := strings.Split(text, " ")
		if len(cli) == 0 {
			continue
		}
		command := strings.ToLower(cli[0])
		var feature feature = &noopFeature{}
		switch command {
		case "help":
			feature = &helpFeature{}
		case "exit":
			fallthrough
		case "quit":
			feature = &exitFeature{isRunning: &isRunning}
		case "payers":
			feature = &payersFeature{base: baseFeature{transactions: data}}
		case "recipients":
			feature = &recipientsFeature{base: baseFeature{transactions: data}}
		case "quarters":
			feature = &quartersFeature{base: baseFeature{transactions: data}}
		case "load":
			feature = &loadFeature{quarters: cli[1:], data: &data}
		case "reload":
			feature = &reloadFeature{base: loadFeature{quarters: cli[1:], data: &data}}
		case "top":
			feature = &topFeature{base: dataFeature{arguments: cli[1:], data: data}}
		case "search":
			feature = &searchFeature{base: dataFeature{arguments: cli[1:], data: data}}
		case "details":
			feature = &detailsFeature{base: dataFeature{arguments: cli[1:], data: data}}
		}
		feature.Run()
	}
	fmt.Println("Bye!")
}
