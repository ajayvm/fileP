package main

import (
	"strconv"
)

type OrganizationPlain struct {
	Index, Founded, NoEmp                   int
	Id, Name, Website, Desc, Industry, Ctry string
}

func ParseOrgFromRecPlain(ptrRec *[]string) (*OrganizationPlain, error) {

	// map the strings
	rec := *ptrRec
	index, err := strconv.Atoi(rec[0])
	if err != nil {
		return nil, err
	}
	id := rec[1]
	name := rec[2]
	website := rec[3]
	ctry := rec[4]
	desc := rec[5]
	founded, err := strconv.Atoi(rec[6])
	if err != nil {
		return nil, err
	}
	ind := rec[7]
	noEmp, err := strconv.Atoi(rec[8])
	if err != nil {
		return nil, err
	}
	org := OrganizationPlain{Index: index, Id: id, Name: name, Website: website, Ctry: ctry, Desc: desc, Founded: founded, Industry: ind, NoEmp: noEmp}
	return &org, nil
}

func ParseOrgFromRec(ptrRec *[]string) (*Organization, error) {

	// map the strings
	rec := *ptrRec
	index, err := strconv.Atoi(rec[0])
	if err != nil {
		return nil, err
	}
	id := rec[1]
	name := rec[2]
	website := rec[3]
	ctry := rec[4]
	desc := rec[5]
	founded, err := strconv.Atoi(rec[6])
	if err != nil {
		return nil, err
	}
	ind := rec[7]
	noEmp, err := strconv.Atoi(rec[8])
	if err != nil {
		return nil, err
	}
	org := Organization{Index: int32(index), Org: id, Name: name, Website: website, Country: ctry, Description: desc, Founded: int32(founded), Industry: ind, NoEmp: int32(noEmp)}
	return &org, nil
}

// func (org fileProto.Organization) ToString() string {
// 	str := strconv.Itoa(org.Index) + ":" + org.Id + ":" + org.Name + ":" + org.Website
// 	return str
// }
