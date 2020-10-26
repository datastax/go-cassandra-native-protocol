package message

const (
	__  = byte('_')
	dot = byte('.')
	_0  = byte('0')
	_1  = byte('1')
	_2  = byte('2')
	_3  = byte('3')
	_4  = byte('4')
	_5  = byte('5')
	_6  = byte('6')
	_7  = byte('7')
	_8  = byte('8')
	_9  = byte('9')
	a   = byte('a')
	b   = byte('b')
	c   = byte('c')
	d   = byte('d')
	e   = byte('e')
	f   = byte('f')
	g   = byte('g')
	h   = byte('h')
	i   = byte('i')
	j   = byte('j')
	k   = byte('k')
	l   = byte('l')
	m   = byte('m')
	n   = byte('n')
	o   = byte('o')
	p   = byte('p')
	q   = byte('q')
	r   = byte('r')
	s   = byte('s')
	t   = byte('t')
	u   = byte('u')
	v   = byte('v')
	w   = byte('w')
	x   = byte('x')
	y   = byte('y')
	z   = byte('z')
	A   = byte('A')
	B   = byte('B')
	C   = byte('C')
	D   = byte('D')
	E   = byte('E')
	F   = byte('F')
	G   = byte('G')
	H   = byte('H')
	I   = byte('I')
	J   = byte('J')
	K   = byte('K')
	L   = byte('L')
	M   = byte('M')
	N   = byte('N')
	O   = byte('O')
	P   = byte('P')
	Q   = byte('Q')
	R   = byte('R')
	S   = byte('S')
	T   = byte('T')
	U   = byte('U')
	V   = byte('V')
	W   = byte('W')
	X   = byte('X')
	Y   = byte('Y')
	Z   = byte('Z')
)

type encodeTestCase struct {
	name     string
	input    Message
	expected []byte
	err      error
}

type decodeTestCase struct {
	name     string
	input    []byte
	expected Message
	err      error
}

type encodedLengthTestCase struct {
	name     string
	input    Message
	expected int
	err      error
}
