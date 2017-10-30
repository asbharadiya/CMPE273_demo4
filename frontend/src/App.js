import React, { Component } from 'react';
import { Route, BrowserRouter, Switch, Redirect } from 'react-router-dom';
import {connect} from 'react-redux';
import './App.css';
import * as actions from './actions';
import Landing from './components/landing/landing';
import Dashboard from './components/dashboard/dashboard';
import Files from './components/dashboard/files';

class App extends Component {

  componentWillMount(){
    this.props.checkSession();
  }

  render() {
    const isLogged = this.props.isLogged;
    return (
      <div>
      {
        isLogged === undefined ? (
          <div className="text-center"><h1>Loading...</h1></div>
        ) : (
          <BrowserRouter>
            <Switch>
              <Route exact path='/' render={() => (
                isLogged ? (
                  <Redirect to="/files"/>
                ) : (
                  <Landing/>
                )
              )}/>
              <Dashboard>
                <Route path='/files' render={() => (
                  !isLogged ? (
                    <Redirect to="/"/>
                  ) : (
                    <Files/>
                  )
                )}/>
              </Dashboard>
            </Switch>
          </BrowserRouter>
        )
      }
      </div>
    );
  }
}

function mapStateToProps(state) {
    return {isLogged:state.isLogged};
}

function mapDispatchToProps(dispatch) {
    return {
        checkSession : () => dispatch(actions.checkSession())
    };
}

export default connect(mapStateToProps,mapDispatchToProps)(App);
