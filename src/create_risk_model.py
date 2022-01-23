import sqlite3
from sklearn.decomposition import PCA
import pandas as pd
import numpy as np
import cvxpy as cvx



def create_pca_factors(returns, svd_solver = "full", exposure_cutoff = .8):
    pca = PCA(n_components=returns.shape[1], svd_solver=svd_solver)
    try:
        first_fit = pca.fit(returns)  
        num_factors_exposure = sum(np.cumsum(first_fit.explained_variance_ratio_)<exposure_cutoff)
        pca = PCA(n_components=num_factors_exposure, svd_solver=svd_solver)
        pca.fit(returns)
        return pca

    except ValueError:
        print(returns)


def factor_betas(pca, factor_beta_indices, factor_beta_columns):
    """
    Get the factor betas from the PCA model.

    Parameters
    ----------
    pca : PCA
        Model fit to returns
    factor_beta_indices : 1 dimensional Ndarray
        Factor beta indices
    factor_beta_columns : 1 dimensional Ndarray
        Factor beta columns

    Returns
    -------
    factor_betas : DataFrame
        Factor betas
    """
    assert len(factor_beta_indices.shape) == 1
    assert len(factor_beta_columns.shape) == 1
    
    beta_components = pca.components_.T
    
    out = pd.DataFrame(beta_components, columns= factor_beta_columns, index=factor_beta_indices)
    
    return out


def factor_returns(pca, returns, factor_return_indices, factor_return_columns):
    """
    Get the factor returns from the PCA model.

    Parameters
    ----------
    pca : PCA
        Model fit to returns
    returns : DataFrame
        Returns for each ticker and date
    factor_return_indices : 1 dimensional Ndarray
        Factor return indices
    factor_return_columns : 1 dimensional Ndarray
        Factor return columns

    Returns
    -------
    factor_returns : DataFrame
        Factor returns
    """
    assert len(factor_return_indices.shape) == 1
    assert len(factor_return_columns.shape) == 1
    
    f_returns = pca.transform(returns)
    
    out = pd.DataFrame(f_returns, columns=factor_return_columns, index=factor_return_indices)
    return out


def factor_cov_matrix(factor_returns, daily_factor=24):
    """
    Get the factor covariance matrix

    Parameters
    ----------
    factor_returns : DataFrame
        Factor returns
    ann_factor : int
        Annualization factor

    Returns
    -------
    factor_cov_matrix : 2 dimensional Ndarray
        Factor covariance matrix
    """
    
    #TODO: Implement function

    out = np.diag(factor_returns.var(axis=0, ddof=1) * daily_factor)
    
    return out



def idiosyncratic_var_matrix(returns, factor_returns, factor_betas, daily_factor=24):
    """
    Get the idiosyncratic variance matrix

    Parameters
    ----------
    returns : DataFrame
        Returns for each ticker and date
    factor_returns : DataFrame
        Factor returns
    factor_betas : DataFrame
        Factor betas
    ann_factor : int
        Annualization factor

    Returns
    -------
    idiosyncratic_var_matrix : DataFrame
        Idiosyncratic variance matrix
    """

    common_returns = np.dot(factor_returns, factor_betas.T)
    
    residuals = returns - common_returns
    
    out = pd.DataFrame(
        np.diag(
            np.var(residuals)*daily_factor), 
        columns = returns.columns, 
        index=returns.columns)

    return out


def idiosyncratic_var_vector(returns, idiosyncratic_var_matrix):
    """
    Get the idiosyncratic variance vector

    Parameters
    ----------
    returns : DataFrame
        Returns for each ticker and date
    idiosyncratic_var_matrix : DataFrame
        Idiosyncratic variance matrix

    Returns
    -------
    idiosyncratic_var_vector : DataFrame
        Idiosyncratic variance Vector
    """
    
    out = pd.DataFrame(np.diag(idiosyncratic_var_matrix),
                      index = returns.columns)
    
    return out


def predict_portfolio_risk(factor_betas, factor_cov_matrix, idiosyncratic_var_matrix, weights):
    """
    Get the predicted portfolio risk
    
    Formula for predicted portfolio risk is sqrt(X.T(BFB.T + S)X) where:
      X is the portfolio weights
      B is the factor betas
      F is the factor covariance matrix
      S is the idiosyncratic variance matrix

    Parameters
    ----------
    factor_betas : DataFrame
        Factor betas
    factor_cov_matrix : 2 dimensional Ndarray
        Factor covariance matrix
    idiosyncratic_var_matrix : DataFrame
        Idiosyncratic variance matrix
    weights : DataFrame
        Portfolio weights

    Returns
    -------
    predicted_portfolio_risk : float
        Predicted portfolio risk
    """
    assert len(factor_cov_matrix.shape) == 2

    risky_term = factor_betas.dot(factor_cov_matrix).dot(factor_betas.T) + idiosyncratic_var_matrix
    
    
    weights = np.array(weights)
    risky_term = np.array(risky_term)
    
    weighted = (weights.T).dot(risky_term).dot(weights)

    scaled = weighted**.5
    
    return scaled[0][0] ## this indexing is silly, is there a better way to extract the value?


def create_risk_model(returns):
    
    risk_model = {}
    pca = create_pca_factors(returns)

    num_factor_exposures = pca.n_components_
    
    risk_model['factor_betas'] = factor_betas(pca, returns.columns.values, np.arange(num_factor_exposures))
    
    risk_model['factor_returns'] = factor_returns(pca, returns, returns.index, np.arange(num_factor_exposures))
    
    risk_model['factor_cov_matrix'] = factor_cov_matrix(risk_model['factor_returns'])
    
    risk_model['idiosyncratic_var_matrix'] = idiosyncratic_var_matrix(returns, risk_model['factor_returns'], risk_model['factor_betas'])
    
    risk_model['idiosyncratic_var_vector'] = idiosyncratic_var_vector(returns, risk_model['idiosyncratic_var_matrix'])  

    return risk_model

