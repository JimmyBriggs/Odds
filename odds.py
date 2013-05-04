#
#   Handy python module for calculating factorials and odds
#

def factorial( n ):
    """
    factorial(n): return the factorial of the integer n.
        factorial(0) = 1
        factorial(-n) =  -factorial(abs(n))
    """
    result = 1
    for i in xrange(1, abs(n)+1):
        result *= i
        if n < 0:
            result = -result
    return result

def perm( n, k ):
    """
    Returns the Permutation for n over k - P(n,k)
    """
    if not 0 <= k <= n:
        return 0
    if k == 0:
        return 1
    # calculate n!/(n-k)! as one product, avoiding factors that 
    # just get canceled
    P = (n-k)+1
    for i in xrange(P+1, n+1):
        P *= i
    return P

def binomial( n, k ):
    """
    binomial(n, k): return the binomial coefficient - C(n k).
    """
    if not 0 <= k <= n:
        return 0
    if k == 0 or k == n:
        return 1
    # calculate n!/k! as one product, avoiding factors that 
    # just get canceled
    P = k+1
    for i in xrange(k+2, n+1):
        P *= i
    #
    return P / factorial(n-k)


def comb( n, k ):
    """
    Combination(n k), which is identical to the binomial coefficent k, in the polynomial (1+X)^n
    Assumes n and k are integers
    """
    return binomial( n, k )

def exact_match( w, m, n, p ):
    """
    Odds where n balls chosen from a barrel of m balls, where we need w "winners" out of p balls picked
        from http://probability.infarom.ro/lottery.html
        comb(m,n) / ( comb(p,w) * comb(m-p,n-w) )
    """
    # ensure we return a float to capture fractional values
    return (comb(m,n)*1.0) / (comb(p,w) * comb(m-p,n-w))
    
def powerball( division, balls=40, drawn=6, picked=6, powerballs=20 ):
    """
    Returns the odds for each of the 8 divisions in powerballs, based on the total balls used,
    how many are drawn, how many are picked, and if the powerball is required
    Expecting division in (1,2,3,4,5,6,7,8)
    """
    if division == 1:        
        return exact_match( 6, balls, drawn, picked ) * powerballs
    elif division == 2:
        return exact_match( 6, balls, drawn, picked ) * (powerballs/powerballs-1)
    elif division == 3:        
        return exact_match( 5, balls, drawn, picked ) * powerballs
    elif division == 4:
        return exact_match( 5, balls, drawn, picked ) * (powerballs/powerballs-1)
    elif division == 5:        
        return exact_match( 4, balls, drawn, picked ) * powerballs
    elif division == 6:        
        return exact_match( 3, balls, drawn, picked ) * powerballs
    elif division == 7:
        return exact_match( 4, balls, drawn, picked ) * (powerballs/powerballs-1)
    elif division == 8:        
        return exact_match( 2, balls, drawn, picked ) * powerballs
    else:
        print 'Unknown division'
        raise RuntimeError

def powerball_game( division ):
    """
    return a string decsribing the division requirements
    """
    if division == 1:        
        return '6 winning plus powerball'
    elif division == 2:
        return '6 winning               '
    elif division == 3:        
        return '5 winning plus powerball'
    elif division == 4:
        return '5 winning               '
    elif division == 5:        
        return '4 winning plus powerball'
    elif division == 6:        
        return '3 winning plus powerball'
    elif division == 7:
        return '4 winning               '
    elif division == 8:        
        return '2 winning plus powerball'
    else:
        print 'Unknown division'
        raise RuntimeError
    
def main():
    """
    Just print something intelligent
    """
    print '%10s\t%40s\t%10s' % ('Div', 'Required', 'Chance')
    #
    for div in range(1,9):
        #
        print '%10d\t%40s\t%10.2f' % (div, powerball_game(div), powerball(div))
    print
    
    
    
    